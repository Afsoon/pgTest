use std::{future::Future, pin::Pin};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use super::{scheduler::WarmSchedulerError, test_support::BURST, *};

type Scheduler = Pin<Box<dyn Future<Output = Result<(), WarmSchedulerError>> + Send>>;

fn pool(target: u16, capacity: usize, concurrency: usize) -> Arc<ConnectionWarmPool> {
    Arc::new(ConnectionWarmPool::new(
        ConnectionWarmConfig::try_new(
            target,
            capacity,
            concurrency,
            Duration::ZERO,
            BTreeMap::new(),
        )
        .unwrap(),
        "postgres",
    ))
}

fn register(pool: &ConnectionWarmPool, ids: impl IntoIterator<Item = u64>) {
    for id in ids {
        assert!(pool.register_database(DatabaseId(id), format!("physical_{id}")));
    }
}

fn scheduler(pool: &Arc<ConnectionWarmPool>, listener: &TcpListener) -> Scheduler {
    Box::pin(
        pool.clone().run_scheduler("127.0.0.1".to_owned(), listener.local_addr().unwrap().port()),
    )
}

async fn drive<T>(scheduler: &mut Scheduler, work: impl Future<Output = T>) -> T {
    tokio::select! {
        result = scheduler.as_mut() => panic!("scheduler stopped unexpectedly: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(5), work) => {
            result.expect("scheduler/socket scenario must finish")
        }
    }
}

async fn until(mut ready: impl FnMut() -> bool) {
    while !ready() {
        tokio::task::yield_now().await;
    }
}

fn idle_count(pool: &ConnectionWarmPool) -> usize {
    pool.state.lock().unwrap().databases.values().map(|entry| entry.idle.len()).sum()
}

async fn accept_startup(listener: &TcpListener) -> (String, TcpStream) {
    let (mut peer, _) = listener.accept().await.unwrap();
    let length = peer.read_u32().await.unwrap();
    let mut payload = vec![0; length as usize - 4];
    peer.read_exact(&mut payload).await.unwrap();
    assert_eq!(&payload[..4], &196608_u32.to_be_bytes());
    let fields: Vec<_> =
        std::str::from_utf8(&payload[4..]).unwrap().trim_end_matches('\0').split('\0').collect();
    let params: BTreeMap<_, _> = fields.chunks_exact(2).map(|pair| (pair[0], pair[1])).collect();
    assert_eq!(params["user"], "postgres");
    (params["database"].to_owned(), peer)
}

async fn reply(peer: &mut TcpStream, success: bool) {
    peer.write_all(b"R\0\0\0\x08\0\0\0\0").await.unwrap();
    peer.write_all(if success { BURST } else { b"E\0\0\0\x04" }).await.unwrap();
}

async fn closed(mut peer: TcpStream) {
    let result = tokio::time::timeout(Duration::from_secs(5), peer.read(&mut [0]))
        .await
        .expect("discarded socket must close");
    match result {
        Ok(count) => assert_eq!(count, 0),
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
    }
}

async fn stop(pool: &ConnectionWarmPool, scheduler: &mut Scheduler) {
    pool.cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), scheduler.as_mut()).await.unwrap().unwrap();
    let state = pool.state.lock().unwrap();
    assert!(state.databases.values().all(|entry| entry.in_flight == 0));
    assert_eq!(
        state.capacity_used,
        state.databases.values().map(|entry| entry.idle.len()).sum::<usize>()
    );
    assert_eq!(pool.attempt_permits.available_permits(), pool.config.attempt_limit());
}

async fn advance(scheduler: &mut Scheduler, duration: Duration) {
    tokio::time::pause();
    tokio::time::advance(duration).await;
    assert!(futures::poll!(scheduler.as_mut()).is_pending());
    tokio::time::resume();
}

#[tokio::test]
async fn scheduler_warms_round_robin_and_replenishes_after_checkout_and_registration() {
    let pool = pool(2, 6, 1);
    register(&pool, [1, 2, 3]);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut scheduler = scheduler(&pool, &listener);
    let mut peers = Vec::new();
    for (index, id) in [1, 2, 3, 1, 2, 3].into_iter().enumerate() {
        let (name, mut peer) = drive(&mut scheduler, accept_startup(&listener)).await;
        assert_eq!(name, format!("physical_{id}"));
        reply(&mut peer, true).await;
        drive(&mut scheduler, until(|| idle_count(&pool) == index + 1)).await;
        peers.push(peer);
    }
    assert!(futures::poll!(scheduler.as_mut()).is_pending());
    assert!(futures::poll!(Box::pin(listener.accept()).as_mut()).is_pending());

    drop(pool.try_checkout(DatabaseId(2), pool.profile.parameters()).unwrap());
    let (name, mut replacement) = drive(&mut scheduler, accept_startup(&listener)).await;
    assert_eq!(name, "physical_2");
    reply(&mut replacement, true).await;
    drive(&mut scheduler, until(|| idle_count(&pool) == 6)).await;
    peers.push(replacement);

    assert!(pool.retire_database(DatabaseId(1)));
    register(&pool, [4]);
    for expected_idle in [5, 6] {
        let (name, mut peer) = drive(&mut scheduler, accept_startup(&listener)).await;
        assert_eq!(name, "physical_4");
        reply(&mut peer, true).await;
        drive(&mut scheduler, until(|| idle_count(&pool) == expected_idle)).await;
        peers.push(peer);
    }
    stop(&pool, &mut scheduler).await;
    for id in [2, 3, 4] {
        pool.retire_database(DatabaseId(id));
    }
    for peer in peers {
        closed(peer).await;
    }
}

#[tokio::test]
async fn scheduler_bounds_reserved_work_and_releases_attempts_on_retirement_and_stop() {
    let pool = pool(4, 5, 2);
    register(&pool, 1..=20);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut scheduler = scheduler(&pool, &listener);
    let (first_name, first) = drive(&mut scheduler, accept_startup(&listener)).await;
    let (second_name, second) = drive(&mut scheduler, accept_startup(&listener)).await;
    assert_eq!(BTreeMap::from([(first_name.clone(), ()), (second_name.clone(), ())]).len(), 2);
    assert!(matches!(first_name.as_str(), "physical_1" | "physical_2"));
    assert!(matches!(second_name.as_str(), "physical_1" | "physical_2"));
    assert!(futures::poll!(scheduler.as_mut()).is_pending());
    assert_eq!(pool.state.lock().unwrap().capacity_used, 2, "no backlog of reserved futures");
    assert!(futures::poll!(Box::pin(listener.accept()).as_mut()).is_pending());

    let retired = if first_name == "physical_1" { 1 } else { 2 };
    pool.retire_database(DatabaseId(retired));
    let (name, replacement) = drive(&mut scheduler, accept_startup(&listener)).await;
    assert_eq!(name, "physical_3");
    closed(first).await;
    assert_eq!(pool.state.lock().unwrap().capacity_used, 2);
    stop(&pool, &mut scheduler).await;
    closed(second).await;
    closed(replacement).await;
    assert_eq!(pool.state.lock().unwrap().capacity_used, 0);
}

#[tokio::test]
async fn scheduler_retries_on_deadline_while_other_databases_progress_and_retirement_stops_retry() {
    let pool = pool(1, 2, 1);
    register(&pool, [1, 2]);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut scheduler = scheduler(&pool, &listener);
    let (name, mut failed) = drive(&mut scheduler, accept_startup(&listener)).await;
    assert_eq!(name, "physical_1");
    reply(&mut failed, false).await;
    let (name, mut healthy) = drive(&mut scheduler, accept_startup(&listener)).await;
    assert_eq!(name, "physical_2");
    reply(&mut healthy, true).await;
    drive(&mut scheduler, until(|| idle_count(&pool) == 1)).await;
    closed(failed).await;
    let first_deadline = pool.state.lock().unwrap().databases[&DatabaseId(1)].retry_at.unwrap();
    assert!(futures::poll!(scheduler.as_mut()).is_pending());
    advance(
        &mut scheduler,
        first_deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(10),
    )
    .await;
    let (name, mut retried) = drive(&mut scheduler, accept_startup(&listener)).await;
    assert_eq!(name, "physical_1", "timer alone must restart an eligible attempt");
    let before = Instant::now();
    reply(&mut retried, false).await;
    drive(
        &mut scheduler,
        until(|| {
            pool.state.lock().unwrap().databases[&DatabaseId(1)]
                .retry_at
                .is_some_and(|at| at > first_deadline)
        }),
    )
    .await;
    let after = Instant::now();
    let next = pool.state.lock().unwrap().databases[&DatabaseId(1)].retry_at.unwrap();
    assert!(next >= before + Duration::from_secs(2) && next <= after + Duration::from_secs(2));
    closed(retried).await;

    drop(pool.try_checkout(DatabaseId(2), pool.profile.parameters()).unwrap());
    let (name, mut replacement) = drive(&mut scheduler, accept_startup(&listener)).await;
    assert_eq!(name, "physical_2", "cooldown must not block another database");
    reply(&mut replacement, true).await;
    drive(&mut scheduler, until(|| idle_count(&pool) == 1)).await;
    pool.retire_database(DatabaseId(1));
    assert!(futures::poll!(scheduler.as_mut()).is_pending());
    advance(&mut scheduler, Duration::from_secs(31)).await;
    assert!(futures::poll!(Box::pin(listener.accept()).as_mut()).is_pending());
    stop(&pool, &mut scheduler).await;
    pool.retire_database(DatabaseId(2));
    closed(healthy).await;
    closed(replacement).await;
}

#[tokio::test]
async fn expired_retry_waits_for_capacity_then_wakes_on_checkout() {
    let pool = pool(1, 1, 1);
    register(&pool, [1, 2]);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut scheduler = scheduler(&pool, &listener);
    let (_, mut failed) = drive(&mut scheduler, accept_startup(&listener)).await;
    reply(&mut failed, false).await;
    let (name, mut healthy) = drive(&mut scheduler, accept_startup(&listener)).await;
    assert_eq!(name, "physical_2");
    reply(&mut healthy, true).await;
    drive(&mut scheduler, until(|| idle_count(&pool) == 1)).await;
    assert!(futures::poll!(scheduler.as_mut()).is_pending());
    advance(&mut scheduler, Duration::from_secs(31)).await;
    assert!(
        futures::poll!(scheduler.as_mut()).is_pending(),
        "full pool must park even with overdue retry"
    );
    assert!(futures::poll!(Box::pin(listener.accept()).as_mut()).is_pending());
    drop(pool.try_checkout(DatabaseId(2), pool.profile.parameters()).unwrap());
    let (name, retry) = drive(&mut scheduler, accept_startup(&listener)).await;
    assert_eq!(name, "physical_1");
    stop(&pool, &mut scheduler).await;
    closed(failed).await;
    closed(healthy).await;
    closed(retry).await;
}

#[tokio::test]
async fn one_scheduler_owns_attempts_and_dropping_it_allows_restart() {
    let pool = pool(1, 1, 1);
    register(&pool, [1]);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut first = scheduler(&pool, &listener);
    let (_, peer) = drive(&mut first, accept_startup(&listener)).await;
    assert!(matches!(scheduler(&pool, &listener).await, Err(WarmSchedulerError::AlreadyRunning)));
    drop(first);
    closed(peer).await;
    assert_eq!(pool.state.lock().unwrap().capacity_used, 0);
    assert_eq!(pool.attempt_permits.available_permits(), 1);
    let mut second = scheduler(&pool, &listener);
    let (_, peer) = drive(&mut second, accept_startup(&listener)).await;
    stop(&pool, &mut second).await;
    closed(peer).await;
}

#[tokio::test]
async fn disabled_or_closed_scheduler_does_not_open_connections() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let disabled = pool(0, 1, 1);
    assert!(scheduler(&disabled, &listener).await.is_ok());
    let closed_pool = pool(1, 1, 1);
    register(&closed_pool, [1]);
    closed_pool.attempt_permits.close();
    assert!(matches!(
        scheduler(&closed_pool, &listener).await,
        Err(WarmSchedulerError::ConcurrencyClosed)
    ));
    assert_eq!(closed_pool.state.lock().unwrap().capacity_used, 0);
    assert!(futures::poll!(Box::pin(listener.accept()).as_mut()).is_pending());
}

#[tokio::test]
async fn scheduler_replaces_closed_or_noisy_idle_sockets_and_stops_at_retirement() {
    let pool = pool(1, 1, 1);
    register(&pool, [1]);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut scheduler = scheduler(&pool, &listener);
    let (_, mut peer) = drive(&mut scheduler, accept_startup(&listener)).await;
    reply(&mut peer, true).await;
    drive(&mut scheduler, until(|| idle_count(&pool) == 1)).await;

    for data in [None, Some(b"E\0".as_slice()), Some(b"N\0".as_slice())] {
        // Arm idle monitoring, then change only the socket: no checkout,
        // registration, or timer should be required to trigger replenishment.
        assert!(futures::poll!(scheduler.as_mut()).is_pending());
        assert!(futures::poll!(Box::pin(peer.read(&mut [0])).as_mut()).is_pending());
        if let Some(data) = data {
            peer.write_all(data).await.unwrap();
        } else {
            peer.shutdown().await.unwrap();
        }
        let (name, mut replacement) = drive(&mut scheduler, accept_startup(&listener)).await;
        assert_eq!(name, "physical_1");
        closed(peer).await;
        assert_eq!(pool.state.lock().unwrap().capacity_used, 1);
        reply(&mut replacement, true).await;
        drive(&mut scheduler, until(|| idle_count(&pool) == 1)).await;
        peer = replacement;
    }

    assert!(futures::poll!(scheduler.as_mut()).is_pending());
    peer.shutdown().await.unwrap();
    pool.retire_database(DatabaseId(1));
    assert!(futures::poll!(scheduler.as_mut()).is_pending());
    assert_eq!(pool.state.lock().unwrap().capacity_used, 0);
    assert!(futures::poll!(Box::pin(listener.accept()).as_mut()).is_pending());
    closed(peer).await;
    stop(&pool, &mut scheduler).await;
}

#[tokio::test]
async fn armed_monitor_never_consumes_handed_off_traffic_or_closes_client_socket() {
    let pool = pool(1, 1, 1);
    register(&pool, [1]);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut scheduler = scheduler(&pool, &listener);
    let (_, mut peer) = drive(&mut scheduler, accept_startup(&listener)).await;
    reply(&mut peer, true).await;
    drive(&mut scheduler, until(|| idle_count(&pool) == 1)).await;
    assert!(futures::poll!(scheduler.as_mut()).is_pending());
    let mut client = pool.try_checkout(DatabaseId(1), pool.profile.parameters()).unwrap();
    peer.write_all(b"backend traffic").await.unwrap();
    // Force another scheduler iteration after the old idle socket becomes
    // readable. Its stale wakeup must not grant the monitor access to it.
    let (_, replacement) = drive(&mut scheduler, accept_startup(&listener)).await;
    let mut data = [0; 15];
    drive(&mut scheduler, client.stream.read_exact(&mut data)).await.unwrap();
    assert_eq!(&data, b"backend traffic");
    assert_eq!(pool.state.lock().unwrap().capacity_used, 1);
    pool.retire_database(DatabaseId(1));
    stop(&pool, &mut scheduler).await;
    closed(replacement).await;
    client.stream.write_all(b"still owned").await.unwrap();
    let mut data = [0; 11];
    tokio::time::timeout(Duration::from_secs(5), peer.read_exact(&mut data))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&data, b"still owned");
    drop(client);
    closed(peer).await;
}
