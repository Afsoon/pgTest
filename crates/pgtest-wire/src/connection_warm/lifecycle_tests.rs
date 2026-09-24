use std::panic::{AssertUnwindSafe, catch_unwind};

use pgtest::{
    worker_engine::{
        core::{LeaseId, WorkerEngineConfig},
        errors::PostgresDDLClientError,
        lifecycle::{DatabaseDrain, DatabaseLifecycle},
    },
    worker_manager::WorkerEngineManager,
};
use pgtest_database_operations::testcontainer::pg_container_config;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};

use super::{
    test_support::{BURST, assert_peer_closed, ready_session},
    *,
};

fn pool() -> Arc<ConnectionWarmPool> {
    Arc::new(ConnectionWarmPool::new(
        ConnectionWarmConfig::try_new(3, 6, 1, Duration::ZERO, BTreeMap::new()).unwrap(),
        "postgres",
    ))
}

struct ObservedLifecycle {
    pool: Arc<ConnectionWarmPool>,
    entered: mpsc::UnboundedSender<DatabaseId>,
}

impl DatabaseLifecycle for ObservedLifecycle {
    fn database_ready(&self, id: DatabaseId, name: &str) {
        DatabaseLifecycle::database_ready(self.pool.as_ref(), id, name);
    }

    fn database_retired(&self, id: DatabaseId) {
        DatabaseLifecycle::database_retired(self.pool.as_ref(), id);
    }

    fn drain_database(&self, id: DatabaseId) -> DatabaseDrain<'_> {
        Box::pin(async move {
            self.entered.send(id).unwrap();
            self.pool.drain_database(id).await
        })
    }
}

async fn exists(admin: &tokio_postgres::Client, name: &str) -> bool {
    admin
        .query_one("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", &[&name])
        .await
        .unwrap()
        .get(0)
}

async fn deleted(admin: &tokio_postgres::Client, name: &str) {
    while exists(admin, name).await {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn real_cleanup_waits_for_warm_resources_without_blocking_other_leases() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let pool = pool();
        let (tx, mut entered) = mpsc::unbounded_channel();
        let config = pg_container_config().await;
        let template = config.pgtest_pg_database.clone();
        let manager = Arc::new(
            WorkerEngineManager::start_with_lifecycle(
                config,
                WorkerEngineConfig { initial_slots: 2, grow_batch_size: 0, ..Default::default() },
                Arc::new(ObservedLifecycle { pool: pool.clone(), entered: tx }),
            )
            .await
            .unwrap(),
        );
        let lease = manager.attach(&template, LeaseId::new("draining").unwrap()).await.unwrap();
        let id = lease.database_id;
        let cancellation = CancellationToken::new();
        // Initial lifecycle registration must make this reservation possible.
        assert!(
            pool.try_reserve(id)
                .unwrap()
                .establish(&manager.pg_client.host, manager.pg_client.port, &cancellation)
                .await
                .unwrap()
        );

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut running =
            Box::pin(pool.try_reserve(id).unwrap().establish("127.0.0.1", port, &cancellation));
        let mut peer = tokio::select! {
            result = running.as_mut() => panic!("attempt completed early: {result:?}"),
            peer = async {
                let (mut peer, _) = listener.accept().await.unwrap();
                let length = peer.read_u32().await.unwrap();
                let mut startup = vec![0; length as usize - 4];
                peer.read_exact(&mut startup).await.unwrap();
                peer
            } => peer,
        };
        let mut queued =
            Box::pin(pool.try_reserve(id).unwrap().establish("127.0.0.1", port, &cancellation));
        assert!(futures::poll!(queued.as_mut()).is_pending());
        assert_eq!(pool.attempt_permits.available_permits(), 0);

        let (admin, connection) = tokio_postgres::Config::new()
            .host(&manager.pg_client.host)
            .port(manager.pg_client.port)
            .user("postgres")
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        let admin_task = tokio::spawn(connection);
        manager.release(lease.lease_id.clone()).await.unwrap();
        assert_eq!(entered.recv().await.unwrap(), id, "cleanup worker must enter the barrier");
        assert!(exists(&admin, &lease.database_name).await, "physical deletion must wait");
        assert!(lease.cancellation_token().is_cancelled());
        assert!(pool.try_reserve(id).is_none());
        // The unused real PostgreSQL socket closes while deletion is blocked
        // by the deliberately unpolled attempts on our separate socket peer.
        loop {
            let count: i64 = admin
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = $1",
                    &[&lease.database_name.as_ref()],
                )
                .await
                .unwrap()
                .get(0);
            if count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }

        let mut first_waiter = Box::pin(pool.drain_database(id));
        let mut second_waiter = Box::pin(pool.drain_database(id));
        assert!(futures::poll!(first_waiter.as_mut()).is_pending());
        assert!(futures::poll!(second_waiter.as_mut()).is_pending());
        // Dropping one waiter does not undo retirement or steal another wakeup.
        drop(second_waiter);

        let other = manager.attach(&template, LeaseId::new("independent").unwrap()).await.unwrap();
        manager.release(other.lease_id.clone()).await.unwrap();
        assert_eq!(entered.recv().await.unwrap(), other.database_id);
        deleted(&admin, &other.database_name).await;
        assert!(exists(&admin, &lease.database_name).await);
        assert!(matches!(queued.await, Err(WarmAttemptError::Cancelled)));
        assert!(futures::poll!(first_waiter.as_mut()).is_pending());
        assert!(exists(&admin, &lease.database_name).await);
        drop(running); // Aborting the remaining attempt must release the barrier.
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        first_waiter.await.unwrap();
        pool.drain_database(id).await.unwrap();
        assert_eq!(pool.attempt_permits.available_permits(), 1);
        assert_eq!(pool.state.lock().unwrap().capacity_used, 0);
        assert!(!pool.register_database(id, lease.database_name.to_string()));
        deleted(&admin, &lease.database_name).await;
        drop(lease);
        drop(other);
        drop(admin);
        admin_task.await.unwrap().unwrap();
        Arc::try_unwrap(manager).unwrap_or_else(|_| panic!("manager retained")).shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn late_publication_closes_before_drain_and_handoff_is_excluded() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let pool = pool();
        let id = DatabaseId(1);
        pool.register_database(id, "physical".into());
        let (session, mut client_peer) = ready_session(BURST).await;
        assert!(pool.try_reserve(id).unwrap().publish(session));
        let mut client = pool.try_checkout(id, pool.profile.parameters()).unwrap();
        let late = pool.try_reserve(id).unwrap();
        let (late_session, late_peer) = ready_session(BURST).await;
        let mut drain = Box::pin(pool.drain_database(id));
        assert!(futures::poll!(drain.as_mut()).is_pending());
        assert!(!late.publish(late_session));
        drain.await.unwrap();
        assert_peer_closed(late_peer).await;
        assert_eq!(pool.state.lock().unwrap().capacity_used, 0);
        client_peer.write_all(b"owned").await.unwrap();
        let mut data = [0; 5];
        client.stream.read_exact(&mut data).await.unwrap();
        assert_eq!(&data, b"owned");
        drop(client);
        assert_peer_closed(client_peer).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn drain_tracks_removed_idle_sockets_until_disposal_not_just_inventory_counts() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let pool = pool();
        let id = DatabaseId(1);
        pool.register_database(id, "physical".into());
        let (session, peer) = ready_session(BURST).await;
        assert!(pool.try_reserve(id).unwrap().publish(session));
        // Hold the exact ownership window used by health eviction/retirement:
        // inventory removal is done, but disposal outside the lock is pending.
        let removed = {
            let mut state = pool.state.lock().unwrap();
            let removed = state.databases.get_mut(&id).unwrap().idle.pop_front().unwrap();
            state.capacity_used -= 1;
            removed
        };
        let mut drain = Box::pin(pool.drain_database(id));
        assert!(
            futures::poll!(drain.as_mut()).is_pending(),
            "zero inventory is not a completed drain"
        );
        drop(removed);
        drain.await.unwrap();
        assert_peer_closed(peer).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn poisoned_pool_refuses_to_confirm_drain() {
    let pool = pool();
    let id = DatabaseId(1);
    pool.register_database(id, "physical".into());
    let (session, peer) = ready_session(BURST).await;
    assert!(pool.try_reserve(id).unwrap().publish(session));
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _lock = pool.state.lock().unwrap();
            panic!("poison state");
        }))
        .is_err()
    );
    assert!(matches!(
        pool.drain_database(id).await,
        Err(PostgresDDLClientError::DatabaseDrainFailed(_))
    ));
    drop(pool);
    assert_peer_closed(peer).await;
}
