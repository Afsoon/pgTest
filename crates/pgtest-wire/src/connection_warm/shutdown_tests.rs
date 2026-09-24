use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::Ordering,
};

use pgtest::{worker_engine::core::WorkerEngineConfig, worker_manager::WorkerEngineManager};
use pgtest_database_operations::testcontainer::pg_container_config;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use super::{
    test_support::{BURST, assert_peer_closed, ready_session},
    *,
};

fn pool() -> Arc<ConnectionWarmPool> {
    Arc::new(ConnectionWarmPool::new(
        ConnectionWarmConfig::try_new(2, 8, 1, Duration::ZERO, BTreeMap::new()).unwrap(),
        "postgres",
    ))
}

fn assert_stopped(pool: &ConnectionWarmPool) {
    assert!(pool.cancellation.is_cancelled());
    assert!(!pool.scheduler_running.load(Ordering::Acquire));
    assert!(pool.scheduler_drain.is_closed() && pool.scheduler_drain.is_empty());
    assert_eq!(pool.attempt_permits.available_permits(), pool.config.attempt_limit());
    let state = pool.state.lock().unwrap();
    assert_eq!(state.capacity_used, 0);
    assert!(state.schedule_order.is_empty());
    for entry in state.databases.values() {
        assert!(entry.retiring && entry.cancellation.is_cancelled());
        assert!(entry.idle.is_empty());
        assert_eq!(entry.in_flight, 0);
        assert!(entry.retry_at.is_none());
        assert!(entry.drain.is_closed() && entry.drain.is_empty());
    }
}

#[tokio::test]
async fn shutdown_drains_idle_queued_running_and_scheduler_work_but_preserves_handoff() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let pool = pool();
        for id in 1..=4 {
            pool.register_database(DatabaseId(id), format!("physical_{id}"));
        }
        let (idle, idle_peer) = ready_session(BURST).await;
        assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(idle));
        let (handed, mut handed_peer) = ready_session(BURST).await;
        assert!(pool.try_reserve(DatabaseId(4)).unwrap().publish(handed));
        let mut handed = pool.try_checkout(DatabaseId(4), pool.profile.parameters()).unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let reservation = pool.try_reserve(DatabaseId(2)).unwrap();
        let running = tokio::spawn(async move {
            reservation.establish("127.0.0.1", port, &CancellationToken::new()).await
        });
        let (mut peer, _) = listener.accept().await.unwrap();
        let length = peer.read_u32().await.unwrap();
        peer.read_exact(&mut vec![0; length as usize - 4]).await.unwrap();
        let reservation = pool.try_reserve(DatabaseId(3)).unwrap();
        let queued = tokio::spawn(async move {
            reservation.establish("127.0.0.1", port, &CancellationToken::new()).await
        });
        let scheduler = tokio::spawn(pool.clone().run_scheduler("127.0.0.1".into(), port));
        while !pool.scheduler_running.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        let (first, second, database_drain) =
            tokio::join!(pool.shutdown(), pool.shutdown(), pool.drain_database(DatabaseId(2)),);
        first.unwrap();
        second.unwrap();
        database_drain.unwrap();
        scheduler.await.unwrap().unwrap();
        assert!(matches!(running.await.unwrap(), Err(WarmAttemptError::Cancelled)));
        assert!(matches!(queued.await.unwrap(), Err(WarmAttemptError::Cancelled)));
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        assert_peer_closed(idle_peer).await;
        assert_stopped(&pool);
        assert!(futures::poll!(Box::pin(listener.accept()).as_mut()).is_pending());

        handed_peer.write_all(b"still owned").await.unwrap();
        let mut data = [0; 11];
        handed.stream.read_exact(&mut data).await.unwrap();
        assert_eq!(&data, b"still owned");
        drop(handed);
        assert_peer_closed(handed_peer).await;
        let weak = Arc::downgrade(&pool);
        drop(pool);
        assert!(weak.upgrade().is_none(), "background work must not retain the pool");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_shutdown_wait_keeps_admission_closed_and_can_be_resumed() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let pool = pool();
        let id = DatabaseId(1);
        pool.register_database(id, "physical".into());
        let late = pool.try_reserve(id).unwrap();
        let (session, peer) = ready_session(BURST).await;
        let mut shutdown = Box::pin(pool.shutdown());
        assert!(futures::poll!(shutdown.as_mut()).is_pending());
        drop(shutdown);
        assert!(!pool.register_database(DatabaseId(2), "late-ready".into()));
        assert!(pool.try_reserve(id).is_none());
        assert!(pool.try_checkout(id, pool.profile.parameters()).is_none());
        assert!(!late.publish(session));
        let (shutdown, drain) = tokio::join!(pool.shutdown(), pool.drain_database(id));
        shutdown.unwrap();
        drain.unwrap();
        pool.shutdown().await.unwrap();
        assert_peer_closed(peer).await;
        assert_stopped(&pool);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_scheduler_exit_even_without_database_resources() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let pool = pool();
        let mut scheduler = Box::pin(pool.clone().run_scheduler("127.0.0.1".into(), 1));
        assert!(futures::poll!(scheduler.as_mut()).is_pending());
        let late = pool.clone().run_scheduler("127.0.0.1".into(), 1);
        let mut shutdown = Box::pin(pool.shutdown());
        assert!(
            futures::poll!(shutdown.as_mut()).is_pending(),
            "scheduler ownership must settle too"
        );
        drop(scheduler);
        shutdown.await.unwrap();
        // A future constructed before shutdown but first polled afterward must
        // not admit a new scheduler token after the barrier completed.
        late.await.unwrap();
        assert_stopped(&pool);
        let disabled = ConnectionWarmPool::new(ConnectionWarmConfig::default(), "postgres");
        disabled.shutdown().await.unwrap();
        assert_stopped(&disabled);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn poisoned_shutdown_cancels_scheduler_but_does_not_claim_resources_drained() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let pool = pool();
        let mut scheduler = Box::pin(pool.clone().run_scheduler("127.0.0.1".into(), 1));
        assert!(futures::poll!(scheduler.as_mut()).is_pending());
        // Seed after the scheduler parks so an idle socket remains at poison.
        pool.register_database(DatabaseId(1), "physical".into());
        let (session, peer) = ready_session(BURST).await;
        assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(session));
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _lock = pool.state.lock().unwrap();
                panic!("poison state");
            }))
            .is_err()
        );
        let (scheduler_result, shutdown) = tokio::join!(scheduler.as_mut(), pool.shutdown());
        // The scheduler may see poison before shutdown cancellation is polled.
        assert!(
            scheduler_result.is_ok()
                || matches!(
                    scheduler_result,
                    Err(super::scheduler::WarmSchedulerError::StatePoisoned)
                )
        );
        assert!(matches!(shutdown, Err(WarmShutdownError::StatePoisoned)));
        assert!(pool.cancellation.is_cancelled());
        assert!(pool.scheduler_drain.is_closed() && pool.scheduler_drain.is_empty());
        drop(scheduler);
        drop(pool);
        assert_peer_closed(peer).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shutdown_closes_postgres_spares_for_databases_never_assigned_to_a_lease() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let config = pg_container_config().await;
        let pool = Arc::new(ConnectionWarmPool::new(
            ConnectionWarmConfig::try_new(1, 2, 2, Duration::ZERO, BTreeMap::new()).unwrap(),
            "postgres",
        ));
        let manager = WorkerEngineManager::start_with_lifecycle(
            config,
            WorkerEngineConfig { initial_slots: 2, grow_batch_size: 0, ..Default::default() },
            pool.clone(),
        )
        .await
        .unwrap();
        let scheduler = tokio::spawn(
            pool.clone().run_scheduler(manager.pg_client.host.clone(), manager.pg_client.port),
        );
        loop {
            let count: usize =
                pool.state.lock().unwrap().databases.values().map(|entry| entry.idle.len()).sum();
            if count == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let databases: Vec<String> = pool
            .state
            .lock()
            .unwrap()
            .databases
            .values()
            .map(|entry| entry.database_name.clone())
            .collect();
        let (admin, connection) = tokio_postgres::Config::new()
            .host(&manager.pg_client.host)
            .port(manager.pg_client.port)
            .user("postgres")
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        let task = tokio::spawn(connection);
        let count: i64 = admin
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = ANY($1)",
                &[&databases],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 2);
        pool.shutdown().await.unwrap();
        scheduler.await.unwrap().unwrap();
        assert_stopped(&pool);
        loop {
            let count: i64 = admin
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = ANY($1)",
                    &[&databases],
                )
                .await
                .unwrap()
                .get(0);
            if count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let count: i64 = admin
            .query_one("SELECT count(*) FROM pg_database WHERE datname = ANY($1)", &[&databases])
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 2, "pool shutdown closes resources without issuing database DDL");
        manager.shutdown().await;
        drop(admin);
        task.await.unwrap().unwrap();
        let weak = Arc::downgrade(&pool);
        drop(pool);
        assert!(weak.upgrade().is_none());
    })
    .await
    .unwrap();
}
