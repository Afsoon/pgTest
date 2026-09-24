use std::panic::{AssertUnwindSafe, catch_unwind};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{test_support::*, *};

fn pool(per_database: u16, max_total: usize) -> Arc<ConnectionWarmPool> {
    let config =
        ConnectionWarmConfig::try_new(per_database, max_total, 1, Duration::ZERO, BTreeMap::new())
            .unwrap();
    let pool = Arc::new(ConnectionWarmPool::new(config, "postgres"));
    for id in [1, 2] {
        assert!(pool.register_database(DatabaseId(id), format!("physical_{id}")));
    }
    pool
}

#[tokio::test]
async fn publication_at_full_capacity_preserves_the_session_and_its_slot() {
    let pool = pool(1, 1);
    let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
    let before = snapshot(&pool);
    let (session, mut peer) = ready_session(BURST).await;
    assert!(reservation.publish(session));

    let after = snapshot(&pool);
    assert_eq!(after.capacity_used, 1, "consumed reservation must not release the idle slot");
    assert_eq!(after.databases[&1].in_flight, 0);
    assert_eq!(after.databases[&1].bursts, vec![BURST.to_vec()]);
    assert_eq!(after.databases[&1].name, "physical_1");
    assert_eq!(after.databases[&2], before.databases[&2]);

    // Extract only to inspect socket identity. Checkout has its own later step.
    let mut stored = {
        let mut state = pool.state.lock().unwrap();
        let session = state.databases.get_mut(&DatabaseId(1)).unwrap().idle.pop_front().unwrap();
        state.capacity_used -= 1;
        session
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        peer.write_all(b"request").await.unwrap();
        let mut request = [0; 7];
        stored.session.stream.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"request");
        stored.session.stream.write_all(b"reply").await.unwrap();
        let mut reply = [0; 5];
        peer.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"reply");
    })
    .await
    .expect("published session must retain the original connected socket");
    drop(stored);
    assert_peer_closed(peer).await;
}

#[tokio::test]
async fn publishing_multiple_reservations_preserves_other_attempts_and_queue_order() {
    let pool = pool(3, 4);
    let first = pool.try_reserve(DatabaseId(1)).unwrap();
    let second = pool.try_reserve(DatabaseId(1)).unwrap();
    let other = pool.try_reserve(DatabaseId(2)).unwrap();
    let abandoned = pool.try_reserve(DatabaseId(2)).unwrap();
    let (first_session, first_peer) = ready_session(BURST).await;
    let (second_session, second_peer) = ready_session(OTHER_BURST).await;
    let (other_session, other_peer) = ready_session(BURST).await;

    assert!(first.publish(first_session));
    let partial = snapshot(&pool);
    assert_eq!(partial.capacity_used, 4);
    assert_eq!(partial.databases[&1].in_flight, 1);
    assert_eq!(partial.databases[&1].bursts, vec![BURST.to_vec()]);
    assert_eq!(partial.databases[&2].in_flight, 2);
    assert!(partial.databases[&2].bursts.is_empty());

    assert!(other.publish(other_session));
    assert!(second.publish(second_session));
    drop(abandoned);
    let after = snapshot(&pool);
    assert_eq!(after.capacity_used, 3);
    assert_eq!(after.databases[&1].in_flight, 0);
    assert_eq!(after.databases[&1].bursts, vec![BURST.to_vec(), OTHER_BURST.to_vec()]);
    assert_eq!(after.databases[&2].in_flight, 0);
    assert_eq!(after.databases[&2].bursts, vec![BURST.to_vec()]);
    drop(pool);
    for peer in [first_peer, second_peer, other_peer] {
        assert_peer_closed(peer).await;
    }
}

#[tokio::test]
async fn idle_sessions_count_toward_the_per_database_target() {
    let pool = pool(1, 3);
    let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
    let (session, peer) = ready_session(BURST).await;
    assert!(reservation.publish(session));
    let before = snapshot(&pool);
    assert!(pool.try_reserve(DatabaseId(1)).is_none());
    assert_eq!(snapshot(&pool), before);
    let other = pool.try_reserve(DatabaseId(2)).expect("other database still has capacity");
    assert_eq!(snapshot(&pool).capacity_used, 2);
    drop(other);
    assert_eq!(snapshot(&pool), before);
    drop(pool);
    assert_peer_closed(peer).await;
}

#[tokio::test]
async fn idle_sessions_count_toward_global_capacity() {
    let pool = pool(3, 1);
    let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
    let (session, peer) = ready_session(BURST).await;
    assert!(reservation.publish(session));
    let before = snapshot(&pool);
    for id in [1, 2] {
        assert!(pool.try_reserve(DatabaseId(id)).is_none());
        assert_eq!(snapshot(&pool), before);
    }
    drop(pool);
    assert_peer_closed(peer).await;
}

#[tokio::test]
async fn duplicate_registration_preserves_a_populated_idle_queue() {
    let pool = pool(1, 1);
    let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
    let (session, peer) = ready_session(BURST).await;
    assert!(reservation.publish(session));
    let before = snapshot(&pool);
    assert!(!pool.register_database(DatabaseId(1), "replacement".to_owned()));
    assert_eq!(snapshot(&pool), before);
    drop(pool);
    assert_peer_closed(peer).await;
}

#[tokio::test]
async fn inactive_publication_closes_the_session_without_changing_state() {
    let pool = pool(2, 3);
    let mut reservation = pool.try_reserve(DatabaseId(1)).unwrap();
    {
        // Simulate prior settlement, as in the inactive-reservation Drop test.
        let mut state = pool.state.lock().unwrap();
        state.databases.get_mut(&DatabaseId(1)).unwrap().in_flight -= 1;
        state.capacity_used -= 1;
    }
    reservation.active = false;
    let before = snapshot(&pool);
    let (session, peer) = ready_session(BURST).await;
    assert!(!reservation.publish(session));
    assert_eq!(snapshot(&pool), before);
    assert_peer_closed(peer).await;
}

#[tokio::test]
async fn poisoned_publication_closes_the_session_without_recovering_state() {
    let pool = pool(2, 3);
    let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
    let (session, peer) = ready_session(BURST).await;
    let poisoned = catch_unwind(AssertUnwindSafe(|| {
        let _guard = pool.state.lock().unwrap();
        panic!("simulate a failed pool state transition");
    }));
    let before = snapshot(&pool);
    let result = catch_unwind(AssertUnwindSafe(|| reservation.publish(session)));
    assert!(poisoned.is_err());
    assert!(matches!(result, Ok(false)));
    assert!(pool.state.is_poisoned());
    assert_eq!(snapshot(&pool), before);
    assert_peer_closed(peer).await;
}

async fn assert_invalid_publication(corrupt: impl FnOnce(&mut WarmPoolState)) {
    let pool = pool(2, 3);
    let reservation = pool.try_reserve(DatabaseId(1)).unwrap();
    let (session, peer) = ready_session(BURST).await;
    {
        let mut state = pool.state.lock().unwrap();
        corrupt(&mut state);
    }
    let before = snapshot(&pool);
    let result = catch_unwind(AssertUnwindSafe(|| reservation.publish(session)));
    assert!(result.is_err(), "broken reservation invariants must be detected");
    assert!(pool.state.is_poisoned(), "an invalid transition must disable further pool access");
    assert_eq!(snapshot(&pool), before, "validation must precede queue and counter changes");
    assert_peer_closed(peer).await;
}

#[tokio::test]
async fn publication_rejects_missing_database_before_changing_state() {
    assert_invalid_publication(|state| {
        state.databases.remove(&DatabaseId(1));
    })
    .await;
}

#[tokio::test]
async fn publication_rejects_zero_global_capacity_before_changing_state() {
    assert_invalid_publication(|state| {
        state.capacity_used = 0;
    })
    .await;
}

#[tokio::test]
async fn publication_rejects_zero_in_flight_before_changing_state() {
    assert_invalid_publication(|state| {
        state.databases.get_mut(&DatabaseId(1)).unwrap().in_flight = 0;
    })
    .await;
}
