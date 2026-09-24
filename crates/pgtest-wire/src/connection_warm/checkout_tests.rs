use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Barrier,
    thread,
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{test_support::*, *};

fn params() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("user".to_owned(), "postgres".to_owned()),
        ("application_name".to_owned(), "vitest".to_owned()),
        ("options".to_owned(), "-c search_path=public".to_owned()),
        ("client_encoding".to_owned(), "UTF8".to_owned()),
    ])
}

fn pool(per_database: u16, max_total: usize) -> Arc<ConnectionWarmPool> {
    let mut startup_params = params();
    startup_params.remove("user"); // Checkout must match the resolved default user.
    let config =
        ConnectionWarmConfig::try_new(per_database, max_total, 1, Duration::ZERO, startup_params)
            .unwrap();
    let pool = Arc::new(ConnectionWarmPool::new(config, "postgres"));
    for id in [1, 2] {
        // Equal names deliberately demonstrate that the key is DatabaseId.
        assert_eq!(pool.register_database(DatabaseId(id), "physical".to_owned()), per_database > 0);
    }
    pool
}

#[test]
fn unknown_and_empty_databases_miss_without_changing_state() {
    let pool = pool(1, 2);
    let before = snapshot(&pool);
    assert!(pool.try_checkout(DatabaseId(99), &params()).is_none());
    assert!(pool.try_checkout(DatabaseId(1), &params()).is_none());
    assert_eq!(snapshot(&pool), before);

    let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
    let before = snapshot(&pool);
    // An in-flight attempt is not an idle session; checkout must not wait for
    // it.
    assert!(pool.try_checkout(DatabaseId(1), &params()).is_none());
    assert_eq!(snapshot(&pool), before);
    drop(reservation);
}

#[tokio::test]
async fn disabled_checkout_preserves_even_a_seeded_idle_session() {
    let pool = pool(0, 1);
    let (session, peer) = ready_session(BURST).await;
    {
        // Registration cannot populate a disabled pool. Seed a spare only to
        // exercise checkout's own disabled guard independently of registration.
        let mut state = pool.state.lock().unwrap();
        state.databases.insert(
            DatabaseId(1),
            DatabaseWarmState {
                database_name: "physical".to_owned(),
                idle: VecDeque::from([IdleSession { session, _drain: TaskTracker::new().token() }]),
                in_flight: 0,
                retry_at: None,
                retry_strategy: warm_retry_strategy(),
                retiring: false,
                cancellation: pool.cancellation.child_token(),
                drain: TaskTracker::new(),
            },
        );
        state.capacity_used = 1;
    }
    let before = snapshot(&pool);
    assert!(pool.try_checkout(DatabaseId(1), &params()).is_none());
    assert_eq!(snapshot(&pool), before);
    drop(pool);
    assert_peer_closed(peer).await;
}

#[tokio::test]
async fn checkout_preserves_socket_and_startup_bytes_after_pool_drop() {
    let pool = pool(1, 1);
    let (session, mut peer) = ready_session(BURST).await;
    assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(session));
    let mut client_params = params();
    client_params.insert("database".to_owned(), "client_route".to_owned());
    let original_params = client_params.clone();
    let original_profile = pool.profile.clone();

    let mut session = pool.try_checkout(DatabaseId(1), &client_params).expect("matching spare");
    assert_eq!(session.session_burst.bytes(), BURST);
    assert_eq!(client_params, original_params);
    assert_eq!(pool.profile, original_profile);
    assert_eq!(snapshot(&pool).capacity_used, 0);
    assert!(pool.try_checkout(DatabaseId(1), &client_params).is_none());
    drop(pool);

    tokio::time::timeout(Duration::from_secs(5), async {
        peer.write_all(b"backend").await.unwrap();
        let mut data = [0; 7];
        session.stream.read_exact(&mut data).await.unwrap();
        assert_eq!(&data, b"backend");
        session.stream.write_all(b"client").await.unwrap();
        let mut data = [0; 6];
        peer.read_exact(&mut data).await.unwrap();
        assert_eq!(&data, b"client");
    })
    .await
    .expect("checked-out session must own the original socket independently of the pool");
    drop(session);
    assert_peer_closed(peer).await;
}

#[tokio::test]
async fn mismatched_and_replication_clients_leave_the_spare_available() {
    let pool = pool(1, 1);
    let (session, peer) = ready_session(BURST).await;
    assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(session));
    let before = snapshot(&pool);
    let original_profile = pool.profile.clone();
    let matching = params();
    let mut mismatches = vec![];
    for key in matching.keys() {
        let mut missing = matching.clone();
        missing.remove(key);
        mismatches.push(missing);
        let mut changed = matching.clone();
        changed.insert(key.clone(), "different".to_owned());
        mismatches.push(changed);
    }
    let mut extra = matching.clone();
    extra.insert("extra".to_owned(), String::new());
    mismatches.push(extra);
    for value in ["", "false", "true", "database"] {
        let mut replication = matching.clone();
        replication.insert("replication".to_owned(), value.to_owned());
        mismatches.push(replication);
    }
    for client_params in mismatches {
        let original = client_params.clone();
        assert!(pool.try_checkout(DatabaseId(1), &client_params).is_none());
        assert_eq!(client_params, original);
        assert_eq!(pool.profile, original_profile);
        assert_eq!(snapshot(&pool), before);
    }
    let session = pool.try_checkout(DatabaseId(1), &matching).expect("mismatches must not consume");
    assert_eq!(session.session_burst.bytes(), BURST);
    drop(session);
    assert_peer_closed(peer).await;
}

#[tokio::test]
async fn checkout_uses_the_explicit_profile_user_including_empty() {
    for user in ["custom_user", ""] {
        let mut matching = params();
        matching.insert("user".to_owned(), user.to_owned());
        let config =
            ConnectionWarmConfig::try_new(1, 1, 1, Duration::ZERO, matching.clone()).unwrap();
        let pool = Arc::new(ConnectionWarmPool::new(config, "postgres"));
        assert!(pool.register_database(DatabaseId(1), "physical".to_owned()));
        let (session, peer) = ready_session(BURST).await;
        assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(session));
        let before = snapshot(&pool);
        assert!(pool.try_checkout(DatabaseId(1), &params()).is_none());
        assert_eq!(snapshot(&pool), before);
        let session = pool.try_checkout(DatabaseId(1), &matching).expect("explicit user matches");
        assert_eq!(session.session_burst.bytes(), BURST);
        drop(session);
        assert_peer_closed(peer).await;
    }
}

#[tokio::test]
async fn checkout_is_fifo_and_never_borrows_from_another_database_id() {
    let pool = pool(2, 2);
    let (first, first_peer) = ready_session(BURST).await;
    let (second, second_peer) = ready_session(OTHER_BURST).await;
    assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(first));
    assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(second));
    let before = snapshot(&pool);
    for id in [2, 99] {
        assert!(pool.try_checkout(DatabaseId(id), &params()).is_none());
        assert_eq!(snapshot(&pool), before);
    }
    let first = pool.try_checkout(DatabaseId(1), &params()).unwrap();
    assert_eq!(first.session_burst.bytes(), BURST);
    let after_first = snapshot(&pool);
    assert_eq!(after_first.capacity_used, 1);
    assert_eq!(after_first.databases[&1].bursts, vec![OTHER_BURST.to_vec()]);
    assert_eq!(after_first.databases[&2], before.databases[&2]);
    let second = pool.try_checkout(DatabaseId(1), &params()).unwrap();
    assert_eq!(second.session_burst.bytes(), OTHER_BURST);
    assert_eq!(snapshot(&pool).capacity_used, 0);
    assert!(pool.try_checkout(DatabaseId(1), &params()).is_none());
    drop(first);
    drop(second);
    assert_peer_closed(first_peer).await;
    assert_peer_closed(second_peer).await;
}

#[tokio::test]
async fn checkout_releases_capacity_once_without_changing_in_flight_attempts() {
    let pool = pool(2, 3);
    let (session, peer) = ready_session(BURST).await;
    assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(session));
    let pending = pool.try_reserve(DatabaseId(1)).unwrap();
    let other = pool.try_reserve(DatabaseId(2)).unwrap();
    let before = snapshot(&pool);
    assert!(pool.try_reserve(DatabaseId(1)).is_none());
    assert!(pool.try_reserve(DatabaseId(2)).is_none());

    let session = pool.try_checkout(DatabaseId(1), &params()).unwrap();
    let after = snapshot(&pool);
    assert_eq!(after.capacity_used, 2);
    assert_eq!(after.databases[&1].in_flight, 1);
    assert!(after.databases[&1].bursts.is_empty());
    assert_eq!(after.databases[&1].name, before.databases[&1].name);
    assert_eq!(after.databases[&2], before.databases[&2]);
    let replacement =
        pool.try_reserve(DatabaseId(1)).expect("checkout frees both limits immediately");
    let after_refill = snapshot(&pool);
    assert_eq!(after_refill.capacity_used, 3);
    // Client-owned sessions never return to the pool or release a replacement's
    // slot.
    drop(session);
    assert_peer_closed(peer).await;
    assert_eq!(snapshot(&pool), after_refill);
    drop(replacement);
    drop(pending);
    drop(other);
    assert_eq!(snapshot(&pool).capacity_used, 0);
}

#[tokio::test]
async fn poisoned_checkout_misses_without_recovering_or_consuming_state() {
    let pool = pool(1, 1);
    let (session, peer) = ready_session(BURST).await;
    assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(session));
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _guard = pool.state.lock().unwrap();
            panic!("simulate a failed pool transition");
        }))
        .is_err()
    );
    let before = snapshot(&pool);
    let result = catch_unwind(AssertUnwindSafe(|| pool.try_checkout(DatabaseId(1), &params())));
    assert!(matches!(result, Ok(None)));
    assert!(pool.state.is_poisoned());
    assert_eq!(snapshot(&pool), before);
    drop(pool);
    assert_peer_closed(peer).await;
}

#[tokio::test]
async fn checkout_detects_capacity_underflow_before_removing_the_session() {
    let pool = pool(1, 1);
    let (session, peer) = ready_session(BURST).await;
    assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(session));
    pool.state.lock().unwrap().capacity_used = 0;
    let before = snapshot(&pool);
    let result = catch_unwind(AssertUnwindSafe(|| pool.try_checkout(DatabaseId(1), &params())));
    assert!(result.is_err(), "an idle session must occupy capacity");
    assert!(pool.state.is_poisoned(), "validate while the checkout lock is held");
    assert_eq!(snapshot(&pool), before, "underflow must not remove or close the spare");
    drop(result);
    drop(pool);
    assert_peer_closed(peer).await;
}

#[tokio::test]
async fn simultaneous_checkouts_hand_each_session_to_exactly_one_caller() {
    let pool = pool(2, 2);
    let mut peers = vec![];
    for burst in [BURST, OTHER_BURST] {
        let (session, peer) = ready_session(burst).await;
        assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(session));
        peers.push(peer);
    }
    let barrier = Arc::new(Barrier::new(8));
    let callers: Vec<_> = (0..8)
        .map(|_| {
            let pool = pool.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                pool.try_checkout(DatabaseId(1), &params())
            })
        })
        .collect();
    // Retain returned sessions until all callers have completed.
    let sessions: Vec<_> =
        callers.into_iter().filter_map(|caller| caller.join().unwrap()).collect();
    assert_eq!(sessions.len(), 2);
    let mut bursts: Vec<_> =
        sessions.iter().map(|session| session.session_burst.bytes().to_vec()).collect();
    bursts.sort();
    let mut expected = vec![BURST.to_vec(), OTHER_BURST.to_vec()];
    expected.sort();
    assert_eq!(bursts, expected);
    let after = snapshot(&pool);
    assert_eq!(after.capacity_used, 0);
    assert_eq!(after.databases[&1].in_flight, 0);
    assert!(after.databases[&1].bursts.is_empty());
    assert!(pool.try_checkout(DatabaseId(1), &params()).is_none());
    drop(sessions);
    for peer in peers {
        assert_peer_closed(peer).await;
    }
}
