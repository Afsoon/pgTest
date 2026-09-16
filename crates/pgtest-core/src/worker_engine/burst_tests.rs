use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio_util::sync::CancellationToken;

use super::{
    core::{LeaseId, Slot, WorkerEngine, WorkerEngineConfig},
    errors::{AttachError, IOError, PostgresDDLClientError, ReleaseError},
    messages::{ConsumerReply, EngineMessage},
    test_support::{
        ConsumerWorker, PostgresConnection, TestMetrics, WorkerInboxImpl, past_instant,
    },
    traits::EngineIO,
};
use crate::{postgres_manager::PostgresConfig, utils::ReadString};

type Inbox = Arc<Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>;
type Engine =
    WorkerEngine<ConsumerWorker, DeferredIO, WorkerInboxImpl, TestMetrics, PostgresConnection>;

#[derive(Default)]
struct Operations {
    creates: Vec<usize>,
    replacements: Vec<usize>,
    timers: Vec<(EngineMessage<ConsumerWorker>, CancellationToken)>,
    spawn_results: VecDeque<Result<(), IOError>>,
    reject_retry: bool,
}

/// Records work without completing it, so tests can submit an entire burst
/// first.
#[derive(Clone)]
struct DeferredIO {
    inbox: Inbox,
    operations: Arc<Mutex<Operations>>,
}

impl EngineIO<ConsumerWorker, PostgresConnection> for DeferredIO {
    fn spawn_create_database(
        &self,
        index: usize,
        _: Arc<PostgresConnection>,
    ) -> Result<(), IOError> {
        let mut operations = self.operations.lock().unwrap();
        operations.creates.push(index);
        operations.spawn_results.pop_front().unwrap_or(Ok(()))
    }

    fn spawn_recreate_database(
        &self,
        index: usize,
        _: ReadString,
        lease: LeaseId,
        generation: u64,
        _: Arc<PostgresConnection>,
    ) -> Result<(), IOError> {
        self.operations.lock().unwrap().replacements.push(index);
        self.send_message(EngineMessage::DeleteLease { lease, generation })
    }

    fn send_delayed_message(
        &self,
        message: EngineMessage<ConsumerWorker>,
        _: u32,
        cancellation: CancellationToken,
    ) -> Result<(), IOError> {
        self.operations.lock().unwrap().timers.push((message, cancellation));
        Ok(())
    }

    fn send_message(&self, message: EngineMessage<ConsumerWorker>) -> Result<(), IOError> {
        if self.operations.lock().unwrap().reject_retry
            && matches!(message, EngineMessage::RetryDatabaseCreation { .. })
        {
            return Err(IOError::FailedToSendTheMessage);
        }
        self.inbox.lock().unwrap().push_back(message);
        Ok(())
    }
}

struct Fixture {
    engine: Engine,
    io: DeferredIO,
    consumer: ConsumerWorker,
}

impl Fixture {
    fn release(&self, lease: &str) -> EngineMessage<ConsumerWorker> {
        EngineMessage::ReleaseLease { lease: LeaseId::from(lease), reply: self.consumer.clone() }
    }

    async fn new(config: WorkerEngineConfig) -> Self {
        let inbox = Arc::new(Mutex::new(VecDeque::new()));
        let io = DeferredIO { inbox: inbox.clone(), operations: Arc::default() };
        let pg = Arc::new(PostgresConnection::start(PostgresConfig::default()));
        let mut engine = Engine::new(config, pg, io.clone(), WorkerInboxImpl::new(inbox));
        engine.try_init().await;
        Self { engine, io, consumer: ConsumerWorker::new(Arc::default()) }
    }

    fn attach(&self, lease: &str) -> EngineMessage<ConsumerWorker> {
        EngineMessage::AttachOrJoin {
            lease: LeaseId::from(lease),
            reply: self.consumer.clone(),
            message_time: Instant::now(),
        }
    }

    async fn process(&mut self, messages: Vec<EngineMessage<ConsumerWorker>>) {
        self.io.inbox.lock().unwrap().extend(messages);
        self.engine.run().await;
        assert_eq!(
            self.engine.ready_slots.len(),
            self.engine.slots.iter().filter(|slot| matches!(slot, Slot::Ready { .. })).count()
        );
        for &index in &self.engine.pending_creations {
            assert!(matches!(self.engine.slots[index], Slot::Creating { .. } | Slot::Done { .. }));
        }
    }

    async fn complete(&mut self, index: usize) {
        self.process(vec![EngineMessage::TemplateCreated {
            index,
            result: Ok(ReadString::from(format!("completed_{index}"))),
        }])
        .await;
    }

    async fn fail(&mut self, index: usize) {
        self.process(vec![EngineMessage::TemplateCreated {
            index,
            result: Err(PostgresDDLClientError::NonRecoverableError("injected".into())),
        }])
        .await;
    }
}

#[tokio::test]
async fn release_cancels_sessions_and_stays_closed_after_replacement() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1,
        maximum_slots: 1,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("test")]).await;
    let cancellation = fixture.engine.leases["test"].cancellation.clone();
    let generation = fixture.engine.leases["test"].generation;
    fixture.process(vec![fixture.release("test"), fixture.release("test")]).await;
    assert!(cancellation.is_cancelled());
    assert_eq!(fixture.io.operations.lock().unwrap().replacements, vec![0]);
    assert_eq!(
        fixture
            .consumer
            .messages()
            .iter()
            .filter(|reply| matches!(reply, ConsumerReply::ReleaseResult(Ok(()))))
            .count(),
        2
    );
    fixture.complete(0).await;
    fixture.process(vec![fixture.attach("test"), fixture.attach("next")]).await;
    let next_generation = fixture.engine.leases["next"].generation;
    fixture
        .process(vec![
            EngineMessage::Detach { lease: LeaseId::from("test"), generation },
            EngineMessage::LeaseMaxTimeReached { lease: LeaseId::from("test"), generation },
            EngineMessage::DeleteLease { lease: LeaseId::from("test"), generation },
        ])
        .await;
    assert_eq!(fixture.engine.leases["next"].conns, 1);
    assert_eq!(fixture.engine.leases["next"].generation, next_generation);
    assert!(!fixture.engine.leases["next"].cancellation.is_cancelled());
    assert!(
        fixture
            .consumer
            .messages()
            .iter()
            .any(|reply| matches!(reply, ConsumerReply::AttachRejected(AttachError::LeaseClosed)))
    );
}

#[tokio::test]
async fn releasing_an_unseen_id_does_not_allocate_a_database() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1,
        maximum_slots: 1,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.release("late"), fixture.attach("late")]).await;
    assert!(fixture.engine.leases.is_empty());
    assert_eq!(fixture.engine.ready_slots.len(), 1);
    assert!(fixture.io.operations.lock().unwrap().replacements.is_empty());
    assert!(matches!(
        fixture.consumer.messages().back(),
        Some(ConsumerReply::AttachRejected(AttachError::LeaseClosed))
    ));
}

#[tokio::test]
async fn release_fails_all_waiters_without_consuming_the_shared_creation() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0,
        maximum_slots: 1,
        grow_batch_size: 1,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture
        .process(vec![
            fixture.attach("waiting"),
            fixture.attach("waiting"),
            fixture.release("waiting"),
            fixture.attach("survivor"),
        ])
        .await;
    assert_eq!(
        fixture
            .consumer
            .messages()
            .iter()
            .filter(|reply| matches!(
                reply,
                ConsumerReply::AttachRejected(AttachError::LeaseClosed)
            ))
            .count(),
        2
    );
    assert!(fixture.io.operations.lock().unwrap().replacements.is_empty());
    fixture.complete(0).await;
    assert!(fixture.engine.leases.contains_key("survivor"));
    assert!(!fixture.engine.leases.contains_key("waiting"));
    assert!(fixture.engine.waiters.is_empty());
}

#[tokio::test]
async fn record_limit_reserves_room_for_closing_existing_leases() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1,
        maximum_slots: 1,
        max_lease_records: 2,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("active"), fixture.attach("pending")]).await;
    fixture.process(vec![fixture.release("unseen"), fixture.attach("unseen")]).await;
    assert!(fixture.consumer.messages().iter().any(|reply| matches!(
        reply,
        ConsumerReply::ReleaseResult(Err(ReleaseError::LeaseRecordLimitReached))
    )));
    assert!(fixture.consumer.messages().iter().any(|reply| matches!(
        reply,
        ConsumerReply::AttachRejected(AttachError::LeaseRecordLimitReached)
    )));
    fixture
        .process(vec![
            fixture.release("active"),
            fixture.release("pending"),
            fixture.release("active"),
        ])
        .await;
    assert_eq!(
        fixture
            .consumer
            .messages()
            .iter()
            .filter(|reply| matches!(reply, ConsumerReply::ReleaseResult(Ok(()))))
            .count(),
        3
    );
}

#[tokio::test]
async fn lost_release_reply_does_not_undo_closure_or_cleanup() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1,
        maximum_slots: 1,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("test")]).await;
    let cancellation = fixture.engine.leases["test"].cancellation.clone();
    fixture
        .process(vec![EngineMessage::ReleaseLease {
            lease: LeaseId::from("test"),
            reply: ConsumerWorker::failing(Arc::default()),
        }])
        .await;
    fixture.process(vec![fixture.release("test"), fixture.attach("test")]).await;
    assert!(cancellation.is_cancelled());
    assert_eq!(fixture.io.operations.lock().unwrap().replacements, vec![0]);
    assert!(matches!(
        fixture.consumer.messages().back(),
        Some(ConsumerReply::AttachRejected(AttachError::LeaseClosed))
    ));
}

#[tokio::test]
async fn old_generation_events_cannot_retire_a_reused_id() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1,
        maximum_slots: 1,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("test")]).await;
    let generation = fixture.engine.leases["test"].generation;
    fixture
        .process(vec![EngineMessage::LeaseMaxTimeReached {
            lease: LeaseId::from("test"),
            generation,
        }])
        .await;
    fixture.complete(0).await;
    fixture.process(vec![fixture.attach("test")]).await;
    assert_ne!(fixture.engine.leases["test"].generation, generation);
    fixture
        .process(vec![
            EngineMessage::Detach { lease: LeaseId::from("test"), generation },
            EngineMessage::LeaseMaxTimeReached { lease: LeaseId::from("test"), generation },
            EngineMessage::DeleteLease { lease: LeaseId::from("test"), generation },
        ])
        .await;
    assert_eq!(fixture.engine.leases["test"].conns, 1);
    assert!(!fixture.engine.leases["test"].cancellation.is_cancelled());
    assert_eq!(fixture.io.operations.lock().unwrap().replacements, vec![0]);
}

#[tokio::test]
async fn last_disconnect_keeps_the_database_for_reconnect() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1,
        maximum_slots: 1,
        lease_claim_timeout_ms: 0,
        ..WorkerEngineConfig::default()
    })
    .await;
    let lease = LeaseId::from("reconnecting");
    fixture.process(vec![fixture.attach(&lease)]).await;
    let Slot::Leased { db_name: original_database, .. } = fixture.engine.slots[0].clone() else {
        panic!("the first connection must lease the database");
    };

    fixture.process(vec![EngineMessage::Detach { lease: lease.clone(), generation: 1 }]).await;
    assert_eq!(fixture.engine.leases[&lease].conns, 0);
    assert!(matches!(fixture.engine.slots[0], Slot::Leased { .. }));
    assert!(fixture.io.operations.lock().unwrap().replacements.is_empty());
    assert!(fixture.io.operations.lock().unwrap().timers.is_empty());

    fixture.process(vec![fixture.attach(&lease)]).await;
    assert_eq!(fixture.engine.leases[&lease].conns, 1);
    assert!(matches!(
        &fixture.engine.slots[0],
        Slot::Leased { db_name, .. } if *db_name == original_database
    ));
}

#[tokio::test]
async fn startup_does_not_prefill_beyond_initial_size() {
    let fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1,
        maximum_slots: 12,
        starvation_threshold: 8,
        grow_batch_size: 4,
        ..WorkerEngineConfig::default()
    })
    .await;
    assert_eq!(fixture.engine.ready_slots.len(), 1);
    assert_eq!(fixture.engine.capacity.current, 1);
    assert!(fixture.engine.pending_creations.is_empty());
    assert!(fixture.io.operations.lock().unwrap().creates.is_empty());
}

#[tokio::test]
async fn covered_burst_does_not_schedule_redundant_batches() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1,
        maximum_slots: 12,
        starvation_threshold: 0,
        grow_batch_size: 4,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("holder"), fixture.attach("a"), fixture.attach("b")]).await;
    assert_eq!(fixture.engine.capacity.current, 5);
    assert_eq!(fixture.engine.pending_creations.len(), 4);
    assert_eq!(fixture.io.operations.lock().unwrap().creates, vec![1, 2, 3, 4]);
    assert!(fixture.engine.ready_slots.is_empty());
    assert_eq!(fixture.consumer.messages().len(), 1);
}

#[tokio::test]
async fn excess_demand_grows_before_any_completion_and_clamps_to_maximum() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1,
        maximum_slots: 8,
        starvation_threshold: 0,
        grow_batch_size: 4,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("holder")]).await;
    assert_eq!(fixture.engine.pending_creations.len(), 4);
    let burst = (0..10).map(|n| fixture.attach(&format!("waiting_{n}"))).collect();
    fixture.process(burst).await;
    assert_eq!(fixture.engine.capacity.current, 8);
    assert_eq!(fixture.engine.pending_creations.len(), 7);
    assert_eq!(fixture.io.operations.lock().unwrap().creates, (1..8).collect::<Vec<_>>());
    assert_eq!(fixture.consumer.messages().len(), 1, "no creation has completed yet");
}

#[tokio::test]
async fn connections_for_one_lease_share_demand_and_one_completion() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0,
        maximum_slots: 12,
        starvation_threshold: 0,
        grow_batch_size: 4,
        ..WorkerEngineConfig::default()
    })
    .await;
    let burst = (0..20).map(|_| fixture.attach("shared")).collect();
    fixture.process(burst).await;
    assert_eq!(fixture.engine.capacity.current, 4);
    assert_eq!(fixture.engine.waiters.len(), 1);
    fixture.complete(0).await;
    assert_eq!(fixture.consumer.messages().len(), 20);
    assert_eq!(fixture.engine.leases[&LeaseId::from("shared")].conns, 20);
    assert!(fixture.engine.waiters.is_empty());
    assert_eq!(fixture.engine.pending_creations.len(), 3);
}

#[tokio::test]
async fn settled_success_is_not_counted_as_both_ready_and_pending() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0,
        maximum_slots: 8,
        starvation_threshold: 0,
        grow_batch_size: 2,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("a")]).await;
    fixture.complete(0).await;
    fixture.complete(1).await;
    fixture.complete(1).await; // Duplicate completion cannot park the same slot twice.
    assert!(fixture.engine.pending_creations.is_empty());
    assert_eq!(fixture.engine.ready_slots.len(), 1);
    fixture.process(vec![fixture.attach("b")]).await;
    assert_eq!(fixture.engine.capacity.current, 4);
    assert_eq!(fixture.engine.pending_creations.len(), 2);
}

#[tokio::test]
async fn failed_creation_replenishes_without_reusing_failed_slots() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0,
        maximum_slots: 3,
        starvation_threshold: 0,
        grow_batch_size: 1,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("a")]).await;
    fixture.fail(0).await;
    assert_eq!(fixture.engine.capacity.current, 3);
    assert_eq!(fixture.engine.pending_creations.len(), 2);
    assert!(matches!(fixture.engine.slots[0], Slot::Empty));
    fixture.fail(0).await;
    assert_eq!(fixture.engine.counters.template_create_failures, 1);
    fixture.fail(1).await;
    fixture.complete(2).await;
    assert_eq!(fixture.consumer.messages().len(), 1);
    assert!(fixture.engine.pending_creations.is_empty());
    assert_eq!(fixture.io.operations.lock().unwrap().creates, vec![0, 1, 2]);
}

#[tokio::test]
async fn forced_recycling_covers_waiting_demand() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 2,
        maximum_slots: 8,
        starvation_threshold: 0,
        grow_batch_size: 1,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("holder"), fixture.attach("other")]).await;
    let lease = LeaseId::from("holder");
    fixture
        .process(vec![
            EngineMessage::Detach { lease: lease.clone(), generation: 1 },
            EngineMessage::LeaseMaxTimeReached { lease, generation: 1 },
            fixture.attach("waiting"),
        ])
        .await;
    assert_eq!(fixture.engine.capacity.current, 3);
    assert_eq!(fixture.engine.pending_creations.len(), 2);
    assert_eq!(fixture.io.operations.lock().unwrap().creates, vec![2]);
    assert_eq!(fixture.io.operations.lock().unwrap().replacements, vec![0]);
    fixture.complete(0).await;
    assert!(fixture.engine.leases.contains_key(&LeaseId::from("waiting")));
    assert_eq!(fixture.engine.pending_creations.len(), 1);
}

#[tokio::test]
async fn expired_and_undeliverable_groups_do_not_block_live_groups() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 1,
        maximum_slots: 1,
        starvation_threshold: 0,
        grow_batch_size: 1,
        ..WorkerEngineConfig::default()
    })
    .await;
    let failed = ConsumerWorker::failing(Arc::default());
    fixture
        .process(vec![
            fixture.attach("holder"),
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("expired"),
                reply: fixture.consumer.clone(),
                message_time: past_instant(),
            },
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("gone"),
                reply: failed,
                message_time: Instant::now(),
            },
            fixture.attach("live"),
            EngineMessage::Detach { lease: LeaseId::from("holder"), generation: 1 },
            EngineMessage::LeaseMaxTimeReached { lease: LeaseId::from("holder"), generation: 1 },
        ])
        .await;
    fixture.complete(0).await;
    assert_eq!(fixture.consumer.messages().len(), 2);
    assert!(fixture.engine.leases.contains_key(&LeaseId::from("live")));
    assert!(fixture.engine.waiters.is_empty());
    assert_eq!(fixture.engine.counters.waiter_timeouts, 1);
}

#[tokio::test]
async fn expired_groups_do_not_inflate_growth_demand() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0,
        maximum_slots: 8,
        starvation_threshold: 0,
        grow_batch_size: 1,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture
        .process(vec![
            EngineMessage::AttachOrJoin {
                lease: LeaseId::from("expired"),
                reply: fixture.consumer.clone(),
                message_time: past_instant(),
            },
            fixture.attach("live"),
        ])
        .await;
    assert_eq!(fixture.engine.capacity.current, 2, "one live lease plus one spare");
    fixture.complete(0).await;
    assert!(fixture.engine.leases.contains_key(&LeaseId::from("live")));
}

#[tokio::test]
async fn scheduling_retries_keep_one_reservation() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0,
        maximum_slots: 8,
        starvation_threshold: 0,
        grow_batch_size: 1,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture
        .io
        .operations
        .lock()
        .unwrap()
        .spawn_results
        .push_back(Err(IOError::FailedToStartABackgroundProcess(0)));
    fixture.process(vec![fixture.attach("a"), fixture.attach("a")]).await;
    assert_eq!(fixture.engine.capacity.current, 2);
    assert_eq!(fixture.engine.pending_creations.len(), 2);
    assert_eq!(fixture.io.operations.lock().unwrap().creates, vec![0, 1, 0]);
    fixture.complete(0).await;
    assert_eq!(fixture.consumer.messages().len(), 2);
    assert_eq!(fixture.engine.pending_creations.len(), 1);
}

#[tokio::test]
async fn exhausted_scheduling_retries_clear_pending_without_looping() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0,
        maximum_slots: 1,
        starvation_threshold: 0,
        grow_batch_size: 1,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture
        .io
        .operations
        .lock()
        .unwrap()
        .spawn_results
        .extend((0..3).map(|_| Err(IOError::FailedToStartABackgroundProcess(0))));
    fixture.process(vec![fixture.attach("a")]).await;
    assert!(fixture.engine.pending_creations.is_empty());
    assert!(matches!(fixture.engine.slots[0], Slot::Empty));
    assert_eq!(fixture.engine.capacity.current, 1);
    assert_eq!(fixture.engine.counters.unable_to_start_database_slots, 1);
    assert_eq!(fixture.io.operations.lock().unwrap().creates.len(), 3);
}

#[tokio::test]
async fn undeliverable_retry_clears_pending_reservation() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0,
        maximum_slots: 1,
        starvation_threshold: 0,
        grow_batch_size: 1,
        ..WorkerEngineConfig::default()
    })
    .await;
    {
        let mut operations = fixture.io.operations.lock().unwrap();
        operations.reject_retry = true;
        operations.spawn_results.push_back(Err(IOError::FailedToStartABackgroundProcess(0)));
    }
    fixture.process(vec![fixture.attach("a")]).await;
    assert!(fixture.engine.pending_creations.is_empty());
    assert_eq!(fixture.engine.counters.non_ready_slots, 1);
    assert_eq!(fixture.io.operations.lock().unwrap().creates.len(), 1);
}

#[tokio::test]
async fn zero_batch_size_disables_growth() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        initial_slots: 0,
        maximum_slots: 8,
        starvation_threshold: 0,
        grow_batch_size: 0,
        ..WorkerEngineConfig::default()
    })
    .await;
    fixture.process(vec![fixture.attach("a"), fixture.attach("b")]).await;
    assert_eq!(fixture.engine.capacity.current, 0);
    assert!(fixture.engine.pending_creations.is_empty());
    assert!(fixture.io.operations.lock().unwrap().creates.is_empty());
}
