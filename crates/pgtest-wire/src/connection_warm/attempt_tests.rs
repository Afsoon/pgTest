use std::future::Future;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

use super::{test_support::*, *};

const AUTH_OK: &[u8] = b"R\0\0\0\x08\0\0\0\0";

async fn bounded<T>(work: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), work).await.expect("socket scenario must finish")
}

fn pool() -> Arc<ConnectionWarmPool> {
    pool_with_capacity(1, 2)
}

fn pool_with_capacity(per_database: u16, max_total: usize) -> Arc<ConnectionWarmPool> {
    let config = ConnectionWarmConfig::try_new(
        per_database,
        max_total,
        1,
        Duration::ZERO,
        BTreeMap::from([("application_name".to_owned(), "warm-tests".to_owned())]),
    )
    .unwrap();
    let pool = Arc::new(ConnectionWarmPool::new(config, "postgres"));
    for id in [1, 2] {
        assert!(pool.register_database(DatabaseId(id), format!("physical_{id}")));
    }
    pool
}

fn launch(
    pool: &Arc<ConnectionWarmPool>,
    id: u64,
    port: u16,
    cancellation: CancellationToken,
) -> JoinHandle<Result<bool, WarmAttemptError>> {
    let reservation = pool.try_reserve(DatabaseId(id)).unwrap();
    tokio::spawn(async move { reservation.establish("127.0.0.1", port, &cancellation).await })
}

async fn accept_startup(listener: &TcpListener, database: &str) -> TcpStream {
    let (mut peer, _) = listener.accept().await.unwrap();
    let length = peer.read_u32().await.unwrap();
    let mut payload = vec![0; length as usize - 4];
    peer.read_exact(&mut payload).await.unwrap();
    assert_eq!(&payload[..4], &196608_u32.to_be_bytes());
    let fields: Vec<_> =
        std::str::from_utf8(&payload[4..]).unwrap().trim_end_matches('\0').split('\0').collect();
    let params: BTreeMap<_, _> = fields.chunks_exact(2).map(|pair| (pair[0], pair[1])).collect();
    assert_eq!(
        params,
        BTreeMap::from([
            ("database", database),
            ("user", "postgres"),
            ("application_name", "warm-tests"),
        ])
    );
    peer
}

fn assert_empty(pool: &ConnectionWarmPool) {
    let state = snapshot(pool);
    assert_eq!(state.capacity_used, 0);
    for database in state.databases.values() {
        assert_eq!(database.in_flight, 0);
        assert!(database.bursts.is_empty());
    }
    assert_eq!(pool.attempt_permits.available_permits(), 1);
}

async fn assert_closed(mut peer: TcpStream) {
    // Dropping a socket with unread startup bytes can produce a reset rather
    // than an orderly EOF. Both prove the unfinished connection was closed.
    match peer.read(&mut [0]).await {
        Ok(count) => assert_eq!(count, 0),
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
    }
}

#[tokio::test]
async fn attempt_publishes_complete_startup_and_preserves_the_connected_socket() {
    bounded(async {
        let pool = pool();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let task =
            launch(&pool, 1, listener.local_addr().unwrap().port(), CancellationToken::new());
        let mut peer = accept_startup(&listener, "physical_1").await;
        peer.write_all(AUTH_OK).await.unwrap();
        peer.write_all(BURST).await.unwrap();
        assert!(task.await.unwrap().unwrap());

        let state = snapshot(&pool);
        assert_eq!(state.capacity_used, 1);
        assert_eq!(state.databases[&1].in_flight, 0);
        assert_eq!(state.databases[&1].bursts, vec![BURST.to_vec()]);
        assert_eq!(pool.attempt_permits.available_permits(), 1);

        let mut session = pool.try_checkout(DatabaseId(1), pool.profile.parameters()).unwrap();
        session.stream.write_all(b"client traffic").await.unwrap();
        let mut bytes = [0; 14];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"client traffic");
        assert_empty(&pool);
        drop(session);
        assert_closed(peer).await;
    })
    .await;
}

#[tokio::test]
async fn shared_limit_and_cancellation_release_capacity_for_later_attempts() {
    bounded(async {
        let pool = pool();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let first_cancel = CancellationToken::new();
        let first = launch(&pool, 1, port, first_cancel.clone());
        let peer = accept_startup(&listener, "physical_1").await;

        let second_cancel = CancellationToken::new();
        let reservation = pool.try_reserve(DatabaseId(2)).unwrap();
        let mut second = Box::pin(reservation.establish("127.0.0.1", port, &second_cancel));
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert!(futures::poll!(Box::pin(listener.accept()).as_mut()).is_pending());
        assert_eq!(snapshot(&pool).capacity_used, 2);
        assert_eq!(pool.attempt_permits.available_permits(), 0);

        second_cancel.cancel();
        assert!(matches!(second.await, Err(WarmAttemptError::Cancelled)));
        assert_eq!(snapshot(&pool).capacity_used, 1);
        assert_eq!(snapshot(&pool).databases[&1].in_flight, 1);
        assert_eq!(pool.attempt_permits.available_permits(), 0);
        first_cancel.cancel();
        assert!(matches!(first.await.unwrap(), Err(WarmAttemptError::Cancelled)));
        assert_closed(peer).await;
        assert_empty(&pool);

        for entry in pool.state.lock().unwrap().databases.values() {
            assert!(entry.retry_at.is_none(), "cancellation must not schedule backoff");
        }

        let replacement = launch(&pool, 2, port, CancellationToken::new());
        let mut peer = accept_startup(&listener, "physical_2").await;
        peer.write_all(AUTH_OK).await.unwrap();
        peer.write_all(BURST).await.unwrap();
        assert!(replacement.await.unwrap().unwrap());
        drop(pool.try_checkout(DatabaseId(2), pool.profile.parameters()).unwrap());
        assert_closed(peer).await;
        assert_empty(&pool);
    })
    .await;
}

#[tokio::test]
async fn timeout_covers_authentication_and_ready_for_query_with_one_deadline() {
    bounded(async {
        let pool = pool();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let task =
            launch(&pool, 1, listener.local_addr().unwrap().port(), CancellationToken::new());
        let mut peer = accept_startup(&listener, "physical_1").await;

        // Pause only after actual socket I/O establishes the startup attempt.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(3)).await;
        peer.write_all(AUTH_OK).await.unwrap();
        tokio::time::advance(Duration::from_secs(2)).await;
        // Allow a small timer/scheduling margin, but never auto-advance to a
        // mistakenly reset five-second timeout after AuthenticationOk.
        let result = tokio::time::timeout(Duration::from_millis(10), task)
            .await
            .expect("the attempt must finish at its original five-second deadline")
            .unwrap();
        assert!(matches!(result, Err(WarmAttemptError::TimedOut)));
        let deadline = pool.state.lock().unwrap().databases[&DatabaseId(1)].retry_at.unwrap();
        assert_eq!(deadline, Instant::now() + Duration::from_secs(1));
        tokio::time::resume();
        assert_closed(peer).await;
        assert_empty(&pool);
    })
    .await;
}

#[tokio::test]
async fn rejected_startup_releases_the_reservation_and_permit() {
    bounded(async {
        let pool = pool();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let task =
            launch(&pool, 1, listener.local_addr().unwrap().port(), CancellationToken::new());
        let mut peer = accept_startup(&listener, "physical_1").await;
        peer.write_all(AUTH_OK).await.unwrap();
        peer.write_all(b"E\0\0\0\x04").await.unwrap();
        assert!(matches!(
            task.await.unwrap(),
            Err(WarmAttemptError::Upstream(
                postgres_upstream::UpstreamError::StartupRejected { .. }
            ))
        ));
        assert_closed(peer).await;
        assert_empty(&pool);
    })
    .await;
}

#[tokio::test]
async fn dropping_queued_and_running_attempts_releases_their_resources() {
    bounded(async {
        let pool = pool();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let first = launch(&pool, 1, port, CancellationToken::new());
        let peer = accept_startup(&listener, "physical_1").await;
        let cancel = CancellationToken::new();
        let reservation = pool.try_reserve(DatabaseId(2)).unwrap();
        let mut queued = Box::pin(reservation.establish("127.0.0.1", port, &cancel));
        assert!(futures::poll!(queued.as_mut()).is_pending());
        drop(queued);
        assert_eq!(snapshot(&pool).capacity_used, 1);

        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_closed(peer).await;
        assert_empty(&pool);
    })
    .await;
}

#[tokio::test]
async fn already_cancelled_attempt_never_opens_a_socket() {
    bounded(async {
        let pool = pool();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let task = launch(&pool, 1, listener.local_addr().unwrap().port(), cancellation);
        assert!(matches!(task.await.unwrap(), Err(WarmAttemptError::Cancelled)));
        assert!(futures::poll!(Box::pin(listener.accept()).as_mut()).is_pending());
        assert_empty(&pool);
    })
    .await;
}

async fn reject_with_backoff(
    pool: &Arc<ConnectionWarmPool>,
    listener: &TcpListener,
    expected_delay_secs: u64,
) {
    bounded(async {
        let task = launch(pool, 1, listener.local_addr().unwrap().port(), CancellationToken::new());
        let mut peer = accept_startup(listener, "physical_1").await;
        let before = Instant::now();
        peer.write_all(AUTH_OK).await.unwrap();
        peer.write_all(b"E\0\0\0\x04").await.unwrap();
        assert!(matches!(
            task.await.unwrap(),
            Err(WarmAttemptError::Upstream(
                postgres_upstream::UpstreamError::StartupRejected { .. }
            ))
        ));
        let after = Instant::now();
        let deadline = pool.state.lock().unwrap().databases[&DatabaseId(1)].retry_at.unwrap();
        let delay = Duration::from_secs(expected_delay_secs);
        assert!(deadline >= before + delay && deadline <= after + delay);
        assert_closed(peer).await;
        assert_empty(pool);
    })
    .await;
}

async fn verify_cooldown_and_advance(pool: &Arc<ConnectionWarmPool>) {
    tokio::time::pause();
    let deadline = pool.state.lock().unwrap().databases[&DatabaseId(1)].retry_at.unwrap();
    assert!(pool.try_reserve(DatabaseId(1)).is_none());
    let healthy = pool.reserve_next().expect("a healthy database must still make progress");
    assert_eq!(healthy.database_name(), "physical_2");
    drop(healthy);
    assert_empty(pool);

    tokio::time::advance(deadline - Instant::now() - Duration::from_nanos(1)).await;
    assert!(pool.try_reserve(DatabaseId(1)).is_none(), "deadline has not elapsed");
    tokio::time::advance(Duration::from_nanos(1)).await;
    let reservation = pool.reserve_next().expect("retry must become eligible at its deadline");
    assert_eq!(reservation.database_name(), "physical_1");
    drop(reservation);
    tokio::time::resume();
}

#[tokio::test]
async fn failures_back_off_per_database_and_success_resets_the_strategy() {
    let pool = pool();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();

    // Each delay follows a real rejected startup. No retry retains a permit
    // or reservation while its deadline is in the future.
    for seconds in [1, 2, 4, 8, 16, 30, 30] {
        reject_with_backoff(&pool, &listener, seconds).await;
        assert!(!pool.register_database(DatabaseId(1), "replacement".to_owned()));
        verify_cooldown_and_advance(&pool).await;
        if seconds == 2 {
            // At this point the next failure must still wait four seconds:
            // cancellation must neither reset nor advance the strategy.
            let cancellation = CancellationToken::new();
            cancellation.cancel();
            let task = launch(&pool, 1, listener.local_addr().unwrap().port(), cancellation);
            assert!(matches!(task.await.unwrap(), Err(WarmAttemptError::Cancelled)));
        }
    }

    bounded(async {
        let task =
            launch(&pool, 1, listener.local_addr().unwrap().port(), CancellationToken::new());
        let mut peer = accept_startup(&listener, "physical_1").await;
        peer.write_all(AUTH_OK).await.unwrap();
        peer.write_all(BURST).await.unwrap();
        assert!(task.await.unwrap().unwrap());
        assert!(pool.state.lock().unwrap().databases[&DatabaseId(1)].retry_at.is_none());
        drop(pool.try_checkout(DatabaseId(1), pool.profile.parameters()).unwrap());
        assert_closed(peer).await;
    })
    .await;

    // A failure after successful publication starts again at one second.
    reject_with_backoff(&pool, &listener, 1).await;
    verify_cooldown_and_advance(&pool).await;
}

#[tokio::test]
async fn retirement_cancels_queued_and_running_attempts_by_database() {
    bounded(async {
        let pool = pool();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let first = launch(&pool, 1, port, CancellationToken::new());
        let peer = accept_startup(&listener, "physical_1").await;
        let external = CancellationToken::new();
        let queued = pool.try_reserve(DatabaseId(2)).unwrap();
        let mut queued = Box::pin(queued.establish("127.0.0.1", port, &external));
        assert!(futures::poll!(queued.as_mut()).is_pending());

        assert!(pool.retire_database(DatabaseId(2)));
        assert!(matches!(queued.await, Err(WarmAttemptError::Cancelled)));
        assert!(!external.is_cancelled());
        assert!(!pool.cancellation.is_cancelled());
        assert!(!first.is_finished(), "retiring another database must leave this attempt alive");
        assert_eq!(snapshot(&pool).capacity_used, 1);
        assert!(pool.try_reserve(DatabaseId(2)).is_none());

        assert!(pool.retire_database(DatabaseId(1)));
        assert!(matches!(first.await.unwrap(), Err(WarmAttemptError::Cancelled)));
        assert_closed(peer).await;
        assert_empty(&pool);
        let state = pool.state.lock().unwrap();
        assert!(state.schedule_order.is_empty());
        assert_eq!(state.databases.len(), 2, "retired identities remain registered");
        for entry in state.databases.values() {
            assert!(entry.retiring);
            assert!(entry.cancellation.is_cancelled());
            assert!(entry.retry_at.is_none());
        }
    })
    .await;
}

#[tokio::test]
async fn pool_cancellation_reaches_all_reserved_attempts() {
    bounded(async {
        let pool = pool();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let first = launch(&pool, 1, port, CancellationToken::new());
        let peer = accept_startup(&listener, "physical_1").await;
        let external = CancellationToken::new();
        let queued = pool.try_reserve(DatabaseId(2)).unwrap();
        let mut queued = Box::pin(queued.establish("127.0.0.1", port, &external));
        assert!(futures::poll!(queued.as_mut()).is_pending());

        pool.cancellation.cancel();
        assert!(matches!(queued.await, Err(WarmAttemptError::Cancelled)));
        assert!(matches!(first.await.unwrap(), Err(WarmAttemptError::Cancelled)));
        assert_closed(peer).await;
        assert_empty(&pool);
        assert!(pool.try_reserve(DatabaseId(1)).is_none());
        assert!(pool.reserve_next().is_none());
        assert!(!pool.register_database(DatabaseId(3), "new".to_owned()));
        assert!(!external.is_cancelled());
    })
    .await;
}

async fn take_wakeup(pool: &ConnectionWarmPool) {
    assert!(futures::poll!(Box::pin(pool.changed.notified()).as_mut()).is_ready());
    assert!(pool.state.try_lock().is_ok(), "state must be unlocked when consuming a wakeup");
}

async fn complete_attempt(pool: &Arc<ConnectionWarmPool>, listener: &TcpListener) -> TcpStream {
    let task = launch(pool, 1, listener.local_addr().unwrap().port(), CancellationToken::new());
    let mut peer = accept_startup(listener, "physical_1").await;
    peer.write_all(AUTH_OK).await.unwrap();
    peer.write_all(BURST).await.unwrap();
    assert!(task.await.unwrap().unwrap());
    peer
}

#[tokio::test]
async fn retirement_drains_spares_rejects_late_publication_and_preserves_handed_off_sessions() {
    bounded(async {
        let pool = pool_with_capacity(3, 6);
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        take_wakeup(&pool).await; // Registrations coalesce into one state recheck.
        assert!(!pool.register_database(DatabaseId(1), "duplicate".to_owned()));
        assert!(!pool.retire_database(DatabaseId(99)));
        assert!(futures::poll!(Box::pin(pool.changed.notified()).as_mut()).is_pending());

        let mut client_peer = complete_attempt(&pool, &listener).await;
        take_wakeup(&pool).await;
        let mut client = pool.try_checkout(DatabaseId(1), pool.profile.parameters()).unwrap();
        take_wakeup(&pool).await;
        let idle_peer = complete_attempt(&pool, &listener).await;
        take_wakeup(&pool).await;

        // Hold a real completed upstream session between connection and
        // publication, making the retirement race deterministic.
        let late = pool.try_reserve(DatabaseId(1)).unwrap();
        let (session, late_peer) = tokio::join!(
            postgres_upstream::connect(
                late.database_name(),
                pool.profile.parameters(),
                "127.0.0.1",
                listener.local_addr().unwrap().port(),
            ),
            async {
                let mut peer = accept_startup(&listener, "physical_1").await;
                peer.write_all(AUTH_OK).await.unwrap();
                peer.write_all(BURST).await.unwrap();
                peer
            }
        );
        let session = session.unwrap();
        assert_eq!(snapshot(&pool).capacity_used, 2);
        assert!(pool.retire_database(DatabaseId(1)));
        take_wakeup(&pool).await;
        assert_closed(idle_peer).await;
        assert_eq!(snapshot(&pool).capacity_used, 1, "the late reservation still owns its slot");
        assert!(pool.try_checkout(DatabaseId(1), pool.profile.parameters()).is_none());
        assert!(pool.try_reserve(DatabaseId(1)).is_none());
        assert!(!pool.register_database(DatabaseId(1), "replacement".to_owned()));
        assert!(!pool.retire_database(DatabaseId(1)), "retirement is idempotent");
        assert!(futures::poll!(Box::pin(pool.changed.notified()).as_mut()).is_pending());

        assert!(!late.publish(session));
        take_wakeup(&pool).await;
        assert_closed(late_peer).await;
        assert_empty(&pool);

        client.stream.write_all(b"still live").await.unwrap();
        let mut bytes = [0; 10];
        client_peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"still live");
        drop(client);
        assert_closed(client_peer).await;

        let other = pool.reserve_next().unwrap();
        assert_eq!(other.database_name(), "physical_2");
        drop(other);
        take_wakeup(&pool).await;
        assert_empty(&pool);
    })
    .await;
}

#[tokio::test]
async fn retirement_during_backoff_permanently_disables_retry() {
    let pool = pool();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    reject_with_backoff(&pool, &listener, 1).await;
    assert!(pool.retire_database(DatabaseId(1)));
    assert!(pool.state.lock().unwrap().databases[&DatabaseId(1)].retry_at.is_none());
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(30)).await;
    assert!(pool.try_reserve(DatabaseId(1)).is_none());
    let other = pool.reserve_next().unwrap();
    assert_eq!(other.database_name(), "physical_2");
    drop(other);
    assert_empty(&pool);
    tokio::time::resume();
}
