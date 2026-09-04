pub mod core;
pub mod errors;
pub mod messages;
pub mod traits;

#[cfg(test)]
mod test_support;

#[cfg(all(test, feature = "stable_ids"))]
mod worker_engine_test {

    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use super::test_support::*;
    use crate::worker_engine::{
        core::{LeaseId, PoolCapacity, Slot, WorkerEngineConfig},
        messages::{ConsumerReply, EngineMessage},
    };

    #[tokio::test]
    async fn immediate_available_templates() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let connection_name = LeaseId::from("connection1");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![EngineMessage::AttachOrJoin {
            lease: connection_name.clone(),
            reply: consumer_io.clone(),
            message_time: std::time::Instant::now(),
        }];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let mut messages_received = consumer_io.messages();

        let ConsumerReply::Attached { database_name: attached_database_name } =
            messages_received.pop_front().unwrap()
        else {
            panic!("Unexpected response 1");
        };

        let EngineOutcome { slots, leases, capacity, .. } = outcome;

        let worker_lease_information = leases.get(&connection_name).unwrap();
        let Slot::Leased { db_name, lease } = slots.get(worker_lease_information.slot_idx).unwrap()
        else {
            panic!("Invariant state");
        };

        assert_eq!(*db_name, attached_database_name);
        assert_eq!(*lease, connection_name);
        assert_eq!(capacity.free_slots, 3);
    }

    #[tokio::test]
    async fn starvation_templates() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let connection_name_1 = LeaseId::from("connection1");
        let connection_name_2 = LeaseId::from("connection2");
        let connection_name_3 = LeaseId::from("connection3");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: connection_name_1.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: connection_name_2.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: connection_name_3.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
        ];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let EngineOutcome { capacity, .. } = outcome;

        assert_eq!(capacity.free_slots, 9);
    }

    #[tokio::test]
    async fn join_lease_leases_no_new_worker() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let connection_name = LeaseId::from("connection1");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: connection_name.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: connection_name.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
        ];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let mut messages_received = consumer_io.messages();

        let ConsumerReply::Attached { database_name: first_database_name } =
            messages_received.pop_front().unwrap()
        else {
            panic!("Expected Attached for the first attach");
        };

        assert_eq!(first_database_name, db(1));

        let ConsumerReply::Attached { database_name: join_database_name } =
            messages_received.pop_front().unwrap()
        else {
            panic!("Expected Attached for the join");
        };
        assert_eq!(join_database_name, first_database_name);

        let EngineOutcome { slots, leases, capacity, waiters, counters: _ } = outcome;

        assert_eq!(leases.get(&connection_name).unwrap().conns, 2);

        let leased_slots = slots.iter().filter(|slot| matches!(slot, Slot::Leased { .. })).count();
        assert_eq!(leased_slots, 1);
        assert_eq!(capacity.free_slots, 3);
        assert!(waiters.is_empty());
    }

    #[tokio::test]
    async fn detach_reduces_connections() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let connection_name = LeaseId::from("connection1");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: connection_name.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: connection_name.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::Detach { lease: connection_name.clone() },
        ];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let EngineOutcome { slots, leases, capacity, waiters: _, counters } = outcome;

        let lease_entry = leases.get(&connection_name).unwrap();
        assert_eq!(lease_entry.conns, 1);

        let Slot::Leased { db_name: _, lease } = slots.get(lease_entry.slot_idx).unwrap() else {
            panic!("Slot must stay Leased while connections remain");
        };
        assert_eq!(*lease, connection_name);
        assert_eq!(capacity.free_slots, 3);
        assert_eq!(counters.detach_on_zero, 0);
    }

    #[tokio::test]
    async fn detach_on_zero_connections_is_noop() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let connection_name = LeaseId::from("connection1");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: connection_name.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::Detach { lease: connection_name.clone() },
            EngineMessage::Detach { lease: connection_name.clone() },
        ];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let EngineOutcome { slots, leases, capacity: _, waiters: _, counters } = outcome;

        let lease_entry = leases.get(&connection_name).unwrap();
        assert_eq!(lease_entry.conns, 0);

        assert!(matches!(slots.get(lease_entry.slot_idx).unwrap(), Slot::Leased { .. }));
        assert_eq!(counters.detach_on_zero, 1);
    }

    #[tokio::test]
    async fn attach_after_grace_recycle_rejected() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let connection_name = LeaseId::from("connection1");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: connection_name.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::Detach { lease: connection_name.clone() },
            EngineMessage::GraceExpired { lease: connection_name.clone() },
            EngineMessage::AttachOrJoin {
                lease: connection_name.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
        ];

        let EngineOutcome { counters, leases, slots, .. } =
            EngineSimulator::run(messages).await.unwrap();

        assert!(!leases.contains_key(&connection_name));
        assert!(slots.iter().all(|slot| !matches!(slot, Slot::Leased { .. })));
        assert_eq!(counters.rejected_attach_grace_expired, 1);
    }

    #[tokio::test]
    async fn single_waiter_fulfilled_on_template_created() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let waiting_connection = LeaseId::from("connection5");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection1"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection2"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection3"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection4"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: waiting_connection.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::TemplateCreated { index: 4, result: Ok(db(5)) },
        ];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let mut messages_received = consumer_io.messages();

        for expected in 1..=4 {
            let ConsumerReply::Attached { database_name } = messages_received.pop_front().unwrap()
            else {
                panic!("Expected Attached for initial claim {expected}");
            };

            assert_eq!(database_name, db(expected));
        }

        let ConsumerReply::Attached { database_name: waiter_database_name } =
            messages_received.pop_front().unwrap()
        else {
            panic!("Expected the parked claim to be fulfilled by TemplateCreated");
        };

        assert_eq!(waiter_database_name, db(5));

        let EngineOutcome { slots: _, leases, capacity: _, waiters, counters: _ } = outcome;

        assert!(waiters.is_empty());
        assert!(leases.contains_key(&waiting_connection));
    }

    #[tokio::test]
    async fn second_waiter_served_when_first_times_out() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let timed_out_connection = LeaseId::from("connection5");
        let served_connection = LeaseId::from("connection6");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        // The first waiter carries a backdated message_time (past the claim
        // timeout), so offer_template must skip it and serve the second waiter.
        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection1"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection2"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection3"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection4"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: timed_out_connection.clone(),
                reply: consumer_io.clone(),
                message_time: past_instant(),
            },
            EngineMessage::AttachOrJoin {
                lease: served_connection.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::TemplateCreated { index: 4, result: Ok(db(5)) },
        ];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let mut messages_received = consumer_io.messages();

        let EngineOutcome { slots: _, leases, capacity: _, waiters, counters } = outcome;

        let ConsumerReply::Attached { database_name: survivor_database_name } =
            messages_received.pop_back().unwrap()
        else {
            panic!("Expected Attached for the surviving waiter");
        };

        assert_eq!(survivor_database_name, db(5));
        assert!(leases.contains_key(&served_connection));
        assert!(!leases.contains_key(&timed_out_connection));
        assert_eq!(counters.waiter_timeouts, 1);
        assert!(waiters.is_empty());
    }

    #[tokio::test]
    async fn all_waiters_time_out() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let waiter_a = LeaseId::from("connection5");
        let waiter_b = LeaseId::from("connection6");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        // Both waiters carry backdated message_times: the fresh template lands
        // with nobody valid left to claim it.
        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection1"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection2"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection3"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection4"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: waiter_a.clone(),
                reply: consumer_io.clone(),
                message_time: past_instant(),
            },
            EngineMessage::AttachOrJoin {
                lease: waiter_b.clone(),
                reply: consumer_io.clone(),
                message_time: past_instant(),
            },
            EngineMessage::TemplateCreated { index: 4, result: Ok(db(5)) },
        ];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let EngineOutcome { slots, leases, capacity: _, waiters, counters } = outcome;

        assert!(!leases.contains_key(&waiter_a));
        assert!(!leases.contains_key(&waiter_b));
        assert!(
            slots.iter().any(|slot| matches!(slot, Slot::Ready { db_name } if *db_name == db(5))),
            "Unclaimed template must end up Ready"
        );
        assert_eq!(counters.waiter_timeouts, 2);
        assert!(waiters.is_empty());
    }

    #[tokio::test]
    async fn attach_after_max_lifetime_rejected() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let connection_name = LeaseId::from("connection1");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: connection_name.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::LeaseMaxTimeReached { lease: connection_name.clone() },
            EngineMessage::AttachOrJoin {
                lease: connection_name.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
        ];

        let EngineOutcome { counters, leases, .. } = EngineSimulator::run(messages).await.unwrap();

        assert_eq!(counters.rejected_attach_max_lifetime, 1);
        assert!(!leases.contains_key(&connection_name));
    }

    #[tokio::test]
    async fn messages_after_shutdown_discarded() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let connection_name = LeaseId::from("connection1");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: connection_name.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::Shutdown,
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection2"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::Detach { lease: connection_name.clone() },
        ];

        let _ = EngineSimulator::run(messages).await;

        let mut messages_received = consumer_io.messages();

        // Only the pre-shutdown attach was answered; everything after is
        // discarded.
        assert_eq!(messages_received.len(), 1);
        let ConsumerReply::Attached { database_name: _ } = messages_received.pop_front().unwrap()
        else {
            panic!("Expected the single pre-shutdown reply to be Attached");
        };
    }

    /// The race the cancellation-token design must survive: a GraceExpired
    /// message already in flight when the lease was re-attached must be a
    /// no-op.
    #[tokio::test]
    async fn stale_grace_expired_ignored() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let connection_name = LeaseId::from("connection1");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: connection_name.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::Detach { lease: connection_name.clone() },
            EngineMessage::AttachOrJoin {
                lease: connection_name.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::GraceExpired { lease: connection_name.clone() },
        ];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let EngineOutcome { slots, leases, capacity: _, waiters: _, counters } = outcome;

        let lease_entry = leases.get(&connection_name).unwrap();
        assert_eq!(lease_entry.conns, 1);
        assert!(matches!(slots.get(lease_entry.slot_idx).unwrap(), Slot::Leased { .. }));
        assert_eq!(counters.rejected_attach_grace_expired, 0);
    }

    #[tokio::test]
    async fn template_created_error_keeps_waiter() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let waiting_connection = LeaseId::from("connection5");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection1"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection2"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection3"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection4"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: waiting_connection.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
        ];

        let worker_config = WorkerEngineConfig {
            grow_batch_size: 1,
            maximum_slots: 5,
            ..WorkerEngineConfig::default()
        };

        let successful_connection_before_fail = 1;
        let outcome = EngineSimulator::run_with_failing_pg(
            messages,
            successful_connection_before_fail,
            worker_config,
        )
        .await
        .unwrap();

        let EngineOutcome { slots: _, leases, capacity: _, waiters, counters } = outcome;

        assert!(!leases.contains_key(&waiting_connection));
        assert_eq!(waiters, vec![waiting_connection]);
        assert_eq!(counters.template_create_failures, 1);
    }

    #[tokio::test]
    async fn detach_unknown_lease_ignored() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![EngineMessage::Detach { lease: LeaseId::from("ghost") }];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let messages_received = consumer_io.messages();
        assert_eq!(messages_received.len(), 0);

        let EngineOutcome { slots: _, leases, capacity, waiters: _, counters: _ } = outcome;

        assert!(leases.is_empty());
        assert_eq!(capacity.free_slots, 4);
    }

    #[tokio::test]
    async fn grace_expired_unknown_lease_ignored() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![EngineMessage::GraceExpired { lease: LeaseId::from("ghost") }];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let messages_received = consumer_io.messages();
        assert_eq!(messages_received.len(), 0);

        let EngineOutcome { slots: _, leases, capacity, waiters: _, counters: _ } = outcome;

        assert!(leases.is_empty());
        assert_eq!(capacity.free_slots, 4);
    }

    #[tokio::test]
    async fn duplicate_waiter_joins_and_template_parks() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let waiting_connection = LeaseId::from("connection5");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection1"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection2"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection3"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection4"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: waiting_connection.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: waiting_connection.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::TemplateCreated { index: 4, result: Ok(db(5)) },
            EngineMessage::TemplateCreated { index: 5, result: Ok(db(6)) },
        ];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let mut messages_received = consumer_io.messages();

        for expected in 1..=4 {
            let ConsumerReply::Attached { database_name } = messages_received.pop_front().unwrap()
            else {
                panic!("Expected Attached for initial claim {expected}");
            };
            assert_eq!(database_name, db(expected));
        }

        let ConsumerReply::Attached { database_name } = messages_received.pop_front().unwrap()
        else {
            panic!("Expected Attached for the first waiter");
        };
        assert_eq!(database_name, db(5));

        let ConsumerReply::Attached { database_name } = messages_received.pop_front().unwrap()
        else {
            panic!("Expected Attached for the joining waiter");
        };
        assert_eq!(database_name, db(5));

        let EngineOutcome { slots, leases, capacity: _, waiters, counters: _ } = outcome;

        assert_eq!(leases.get(&waiting_connection).unwrap().conns, 2);
        assert!(
            slots.iter().any(|slot| matches!(slot, Slot::Ready { db_name } if *db_name == db(6))),
            "Unconsumed template must be parked Ready, not leaked"
        );
        assert!(waiters.is_empty());
    }

    #[tokio::test]
    async fn one_template_serves_one_fresh_waiter() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let first_waiter = LeaseId::from("connection5");
        let second_waiter = LeaseId::from("connection6");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection1"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection2"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection3"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection4"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: first_waiter.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: second_waiter.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
        ];

        let worker_config = WorkerEngineConfig {
            grow_batch_size: 1,
            maximum_slots: 5,
            ..WorkerEngineConfig::default()
        };

        let outcome =
            EngineSimulator::run_with_custom_config(messages, worker_config).await.unwrap();

        let mut messages_received = consumer_io.messages();

        for expected in 1..=4 {
            let ConsumerReply::Attached { database_name } = messages_received.pop_front().unwrap()
            else {
                panic!("Expected Attached for initial claim {expected}");
            };
            assert_eq!(database_name, db(expected));
        }

        let ConsumerReply::Attached { database_name } = messages_received.pop_front().unwrap()
        else {
            panic!("Expected Attached for the first waiter only");
        };
        assert_eq!(database_name, db(5));

        let EngineOutcome { slots: _, leases, capacity: _, waiters, counters: _ } = outcome;

        assert!(leases.contains_key(&first_waiter));
        assert!(!leases.contains_key(&second_waiter));
        assert_eq!(waiters, vec![second_waiter]);
    }

    #[test]
    fn pool_free_slots_saturates_at_zero() {
        let mut capacity = PoolCapacity::new(WorkerEngineConfig::default());
        capacity.free_slots = 0;
        capacity.occupy_slot();
        assert_eq!(capacity.free_slots, 0);
    }

    #[tokio::test]
    async fn grow_marks_exact_batch_of_creating_slots() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection1"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection2"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection3"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
        ];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let EngineOutcome { slots, leases: _, capacity, waiters: _, counters: _ } = outcome;

        let ready_slots: Vec<usize> = slots
            .iter()
            .enumerate()
            .filter(|(_, slot)| matches!(slot, Slot::Ready { .. }))
            .map(|(idx, _)| idx)
            .collect();

        assert_eq!(
            ready_slots,
            vec![3, 4, 5, 6, 7],
            "grow must fill exactly current..current+batch"
        );
        assert!(
            !matches!(slots[3], Slot::Creating { .. }),
            "slot 3 is a live initial slot and must not be re-created (off-by-one regression)"
        );
        assert_eq!(capacity.current, 8);
    }

    #[tokio::test]
    async fn failed_join_reply_does_not_count_connection() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let waiting_connection = LeaseId::from("connection5");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());
        let failing_io = ConsumerWorker::failing(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection1"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection2"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection3"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection4"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: waiting_connection.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: waiting_connection.clone(),
                reply: failing_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: waiting_connection.clone(),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::TemplateCreated { index: 4, result: Ok(db(5)) },
            EngineMessage::TemplateCreated { index: 5, result: Ok(db(6)) },
        ];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let mut messages_received = consumer_io.messages();

        assert_eq!(messages_received.len(), 6);

        for expected in 1..=4 {
            let ConsumerReply::Attached { database_name } = messages_received.pop_front().unwrap()
            else {
                panic!("Expected Attached for initial claim {expected}");
            };
            assert_eq!(database_name, db(expected));
        }

        for _ in 0..2 {
            let ConsumerReply::Attached { database_name } = messages_received.pop_front().unwrap()
            else {
                panic!("Expected Attached for the delivered waiters");
            };
            assert_eq!(database_name, db(5));
        }

        let EngineOutcome { slots, leases, capacity: _, waiters, counters: _ } = outcome;

        assert_eq!(
            leases.get(&waiting_connection).unwrap().conns,
            2,
            "the failed reply must not be counted as an active connection"
        );
        assert!(
            slots.iter().any(|slot| matches!(slot, Slot::Ready { db_name } if *db_name == db(6))),
            "second template must be parked Ready"
        );
        assert!(waiters.is_empty());
    }

    #[tokio::test]
    async fn failed_fresh_reply_keeps_worker_ready() {
        let consumer_buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>> =
            Arc::from(Mutex::new(VecDeque::new()));
        let waiting_connection = LeaseId::from("connection5");
        let consumer_io = ConsumerWorker::new(consumer_buffer.clone());
        let failing_io = ConsumerWorker::failing(consumer_buffer.clone());

        let messages = vec![
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection1"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection2"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection3"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("connection4"),
                reply: consumer_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::AttachOrJoin {
                lease: waiting_connection.clone(),
                reply: failing_io.clone(),
                message_time: std::time::Instant::now(),
            },
            EngineMessage::TemplateCreated { index: 4, result: Ok(db(5)) },
        ];

        let outcome = EngineSimulator::run(messages).await.unwrap();

        let messages_received = consumer_io.messages();

        assert_eq!(messages_received.len(), 4);

        let EngineOutcome { slots, leases, capacity: _, waiters, counters: _ } = outcome;

        assert!(
            slots.iter().any(|slot| matches!(slot, Slot::Ready { db_name } if *db_name == db(5))),
            "worker must be rolled back to Ready when the reply cannot be delivered"
        );
        assert!(
            !leases.contains_key(&waiting_connection),
            "no lease may remain for the unreachable consumer"
        );
        assert!(waiters.is_empty(), "the dead waiter must be dropped, not re-parked");
    }
}

/// Tests pinning `grow()`'s retry contract around `spawn_create_database`.
#[cfg(all(test, feature = "stable_ids"))]
mod grow_test {

    use super::test_support::*;
    use crate::{
        utils::ReadString,
        worker_engine::{
            core::{Slot, WorkerEngineConfig},
            errors::IOError,
        },
    };

    #[tokio::test]
    async fn retry_success_marks_slot_ready() {
        let (worker, io) =
            run_grow(vec![Err(IOError::FailedToStartABackgroundProcess(1)), Ok(())]).await;

        let snapshot = worker.snapshot();
        assert!(
            matches!(
                &snapshot.slots[1],
                Slot::Ready { db_name } if *db_name == ReadString::from("grow_1")
            ),
            "slot 1 must be Ready after the retry succeeded, got {:?}",
            snapshot.slots[1]
        );
        assert_eq!(snapshot.counters.unable_to_start_database_slots, 0);
        assert_eq!(io.remaining(), 0);
    }

    #[tokio::test]
    async fn three_failures_keep_slot_empty() {
        let (worker, io) = run_grow(vec![
            Err(IOError::FailedToStartABackgroundProcess(1)),
            Err(IOError::FailedToStartABackgroundProcess(1)),
            Err(IOError::FailedToStartABackgroundProcess(1)),
        ])
        .await;

        let snapshot = worker.snapshot();
        assert!(
            matches!(&snapshot.slots[1], Slot::Creating { db_name } if *db_name == ReadString::from("pending")),
            "slot 1 must be reverted to Empty after 3 failed attempts, got {:?}",
            snapshot.slots[1]
        );
        assert_eq!(snapshot.counters.unable_to_start_database_slots, 1);
        assert_eq!(io.remaining(), 0, "exactly 3 attempts, no more");
    }

    #[tokio::test]
    async fn distinct_error_counts_non_ready_worker() {
        let (worker, io) = run_grow(vec![Err(IOError::FailedToSendTheMessage)]).await;

        let snapshot = worker.snapshot();
        assert_eq!(snapshot.counters.non_ready_slots, 1);
        assert_eq!(snapshot.counters.unable_to_start_database_slots, 0);
        assert_eq!(io.remaining(), 0, "non-retryable error must not be retried");
    }

    #[tokio::test]
    async fn grow_with_space_creates_full_batch() {
        let config = WorkerEngineConfig {
            initial_slots: 1,
            maximum_slots: 8,
            starvation_threshold: 2,
            grow_batch_size: 2,
            ..WorkerEngineConfig::default()
        };

        let (worker, io) = run_grow_with(config, vec![Ok(()), Ok(())]).await;

        let snapshot = worker.snapshot();
        assert!(
            matches!(
                &snapshot.slots[1],
                Slot::Ready { db_name } if *db_name == ReadString::from("grow_1")
            ),
            "slot 1 must be Ready, got {:?}",
            snapshot.slots[1]
        );
        assert!(
            matches!(
                &snapshot.slots[2],
                Slot::Ready { db_name } if *db_name == ReadString::from("grow_2")
            ),
            "slot 2 must be Ready, got {:?}",
            snapshot.slots[2]
        );
        assert_eq!(snapshot.capacity.current, 3, "1 initial + full batch of 2");
        assert_eq!(io.remaining(), 0);
    }

    #[tokio::test]
    async fn grow_clamps_batch_to_remaining_space() {
        let config = WorkerEngineConfig {
            initial_slots: 3,
            maximum_slots: 4,
            starvation_threshold: 4,
            grow_batch_size: 4,
            ..WorkerEngineConfig::default()
        };

        let (worker, io) = run_grow_with(config, vec![Ok(())]).await;

        let snapshot = worker.snapshot();
        assert!(
            matches!(
                &snapshot.slots[3],
                Slot::Ready { db_name } if *db_name == ReadString::from("grow_3")
            ),
            "slot 3 must be Ready, got {:?}",
            snapshot.slots[3]
        );
        assert_eq!(snapshot.capacity.current, 4);
        assert!(snapshot.capacity.is_at_maximum_capacity());
        assert_eq!(io.remaining(), 0);
    }

    #[tokio::test]
    async fn grow_at_maximum_creates_nothing() {
        let config = WorkerEngineConfig {
            initial_slots: 4,
            maximum_slots: 4,
            starvation_threshold: 4,
            grow_batch_size: 4,
            ..WorkerEngineConfig::default()
        };

        let (worker, io) = run_grow_with(config, vec![]).await;

        let snapshot = worker.snapshot();
        assert_eq!(snapshot.capacity.current, 4, "capacity unchanged");
        assert!(
            snapshot.slots.iter().all(|slot| matches!(slot, Slot::Empty)),
            "no slot may leave Empty when the capacity is at maximum"
        );
        assert_eq!(io.remaining(), 0);
    }
}
