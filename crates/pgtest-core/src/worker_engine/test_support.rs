#![cfg(test)]
// Consumed only by the stable_ids-gated suites; silence the other mode.
#![cfg_attr(not(feature = "stable_ids"), allow(dead_code, unused_imports))]

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, atomic::AtomicUsize},
};

use rustc_hash::FxHashMap;
use tokio_util::sync::CancellationToken;

use crate::{
    postgres_manager::{PostgresConfig, PostgresDatabaseName},
    utils::ReadString,
    worker_engine::{
        core::{
            EngineCounters, LeaseEntry, LeaseId, PoolCapacity, Slot, WorkerEngine,
            WorkerEngineConfig,
        },
        errors::{IOError, MetricIOError, PostgresDDLClientError},
        messages::{ConsumerReply, EngineMessage, EngineMetricMessage},
        traits::{ConsumerIO, EngineIO, EngineInbox, MetricIO, PostgresClient},
    },
};

#[derive(Clone, Debug)]
pub struct ConsumerWorker {
    messages_replied: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>>,
    fail_send: bool,
}

impl ConsumerWorker {
    pub fn new(buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>>) -> Self {
        Self { messages_replied: buffer, fail_send: false }
    }

    pub fn failing(buffer: Arc<std::sync::Mutex<VecDeque<ConsumerReply>>>) -> Self {
        Self { messages_replied: buffer, fail_send: true }
    }

    pub fn messages(&self) -> VecDeque<ConsumerReply> {
        self.messages_replied.lock().unwrap().clone()
    }
}

impl ConsumerIO for ConsumerWorker {
    fn reply(self, msg: ConsumerReply) -> Result<(), ConsumerReply> {
        if self.fail_send {
            return Err(msg);
        }
        self.messages_replied.lock().unwrap().push_back(msg);
        Ok(())
    }
}

pub struct WorkerEngineIO<'a> {
    inbox: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
    database_progression_name: &'a PostgresDatabaseName,
    fail: bool,
    messages_pushed: AtomicUsize,
    operations_before_fail: usize,
}

impl<'a> WorkerEngineIO<'a> {
    fn new(
        inbox: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
        database_progression_name: &'a PostgresDatabaseName,
    ) -> Self {
        Self {
            inbox,
            database_progression_name,
            fail: false,
            operations_before_fail: 0,
            messages_pushed: AtomicUsize::new(0),
        }
    }

    fn fail(
        inbox: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
        database_progression_name: &'a PostgresDatabaseName,
        operations_before_fail: usize,
    ) -> Self {
        Self {
            inbox,
            database_progression_name,
            fail: true,
            operations_before_fail,
            messages_pushed: AtomicUsize::new(0),
        }
    }
}

impl<'a> EngineIO<ConsumerWorker, PostgresConnection> for WorkerEngineIO<'a> {
    fn spawn_create_database(
        &self,
        worker_index: usize,
        _postgres_client: Arc<PostgresConnection>,
    ) -> Result<(), IOError> {
        if self.fail
            && self.messages_pushed.load(std::sync::atomic::Ordering::SeqCst)
                == self.operations_before_fail - 1
        {
            self.inbox.lock().unwrap().push_back(EngineMessage::TemplateCreated {
                index: worker_index as usize,
                result: Err(PostgresDDLClientError::NonRecoverableError(String::from(
                    "Error creating a database",
                ))),
            });
        } else {
            self.inbox.lock().unwrap().push_back(EngineMessage::TemplateCreated {
                index: worker_index as usize,
                result: Ok(ReadString::from(
                    self.database_progression_name.generate_database_name(),
                )),
            });
            self.messages_pushed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }

        Ok(())
    }

    fn spawn_recreate_database(
        &self,
        worker_index: usize,
        _database_name: ReadString,
        lease: LeaseId,
        generation: u64,
        _postgres_client: Arc<PostgresConnection>,
    ) -> Result<(), IOError> {
        let mut inbox = self.inbox.lock().unwrap();
        inbox.push_back(EngineMessage::DeleteLease { lease: lease.clone(), generation });
        inbox.push_back(EngineMessage::TemplateCreated {
            index: worker_index,
            result: Ok(ReadString::from(self.database_progression_name.generate_database_name())),
        });

        Ok(())
    }

    fn send_delayed_message(
        &self,
        _msg: EngineMessage<ConsumerWorker>,
        _wait_duration: u32,
        _cancel_token: CancellationToken,
    ) -> Result<(), IOError> {
        // TODO(user): timer semantics undecided — deliberately inert for now.
        // NOTE: max-lifetime timers never fire in the simulator until
        // this is implemented.
        Ok(())
    }

    fn send_message(&self, msg: EngineMessage<ConsumerWorker>) -> Result<(), IOError> {
        self.inbox.lock().unwrap().push_back(msg);

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct WorkerInboxImpl {
    inbox: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
}

impl WorkerInboxImpl {
    pub fn new(buffer: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>) -> Self {
        WorkerInboxImpl { inbox: buffer }
    }

    fn push_message(&self, msg: EngineMessage<ConsumerWorker>) {
        self.inbox.lock().unwrap().push_back(msg);
    }
}

impl EngineInbox<ConsumerWorker> for WorkerInboxImpl {
    fn wait_for_message(
        &mut self,
    ) -> impl Future<Output = Option<EngineMessage<ConsumerWorker>>> + Send {
        std::future::ready(self.inbox.lock().unwrap().pop_front())
    }
}

pub struct TestMetrics {
    _inbox: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
}

impl MetricIO for TestMetrics {
    fn send_metric(
        &self,
        _metric_message: EngineMetricMessage,
    ) -> impl Future<Output = Result<(), MetricIOError>> + Send {
        std::future::ready(Ok(()))
    }
}

pub struct PostgresConnection {
    template_database_name: PostgresDatabaseName,
}

impl PostgresConnection {
    pub(super) fn start(postgres_config: PostgresConfig) -> Self {
        Self {
            template_database_name: PostgresDatabaseName::new(postgres_config.pgtest_pg_database),
        }
    }
}

impl PostgresClient for PostgresConnection {
    async fn create_database(&self) -> Result<ReadString, PostgresDDLClientError> {
        let database_name = self.template_database_name.generate_database_name();
        Ok(ReadString::from(database_name))
    }

    async fn drop_database(&self, _database_name: &str) -> Result<(), PostgresDDLClientError> {
        Ok(())
    }

    async fn drop_templates_like(&self) -> Result<(), PostgresDDLClientError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct EngineOutcome {
    pub counters: EngineCounters,
    pub leases: FxHashMap<LeaseId, LeaseEntry>,
    pub slots: Box<[Slot]>,
    pub capacity: PoolCapacity,
    pub ready_slots: VecDeque<usize>,
    pub waiters: Vec<LeaseId>,
}

pub struct EngineSimulator;

impl EngineSimulator {
    pub async fn run<'a>(
        msgs: Vec<EngineMessage<ConsumerWorker>>,
    ) -> Result<EngineOutcome, IOError> {
        let config = PostgresConfig::default();
        let manager = Arc::from(PostgresConnection::start(config));

        let _ = manager.drop_templates_like().await;

        let engine_config = WorkerEngineConfig::default();

        let inbox_buffer: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>> =
            Arc::from(Mutex::new(VecDeque::new()));

        let inbox = WorkerInboxImpl::new(inbox_buffer.clone());
        let manager_to_bg = manager.clone();
        let engine_io =
            WorkerEngineIO::new(inbox_buffer.clone(), &manager_to_bg.template_database_name);

        for msg in msgs {
            inbox.push_message(msg);
        }

        let mut engine = WorkerEngine::<
            ConsumerWorker,
            WorkerEngineIO,
            WorkerInboxImpl,
            TestMetrics,
            PostgresConnection,
        >::new(engine_config, manager, engine_io, inbox.clone());

        engine.try_init().await;

        engine.run().await;

        Ok(EngineOutcome {
            counters: engine.counters.clone(),
            leases: engine.leases.clone(),
            slots: engine.slots.clone(),
            capacity: engine.capacity.clone(),
            ready_slots: engine.ready_slots.clone(),
            waiters: engine.waiters.iter().map(|lease| lease.clone()).collect(),
        })
    }

    pub async fn run_with_failing_pg<'a>(
        msgs: Vec<EngineMessage<ConsumerWorker>>,
        operations_before_fail: usize,
        worker_engine_config: WorkerEngineConfig,
    ) -> Result<EngineOutcome, IOError> {
        let config = PostgresConfig::default();
        let manager = Arc::from(PostgresConnection::start(config));

        let _ = manager.drop_templates_like().await;

        let inbox_buffer: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>> =
            Arc::from(Mutex::new(VecDeque::new()));

        let inbox = WorkerInboxImpl::new(inbox_buffer.clone());
        let manager_to_bg = manager.clone();
        let engine_io = WorkerEngineIO::fail(
            inbox_buffer.clone(),
            &manager_to_bg.template_database_name,
            operations_before_fail,
        );

        for msg in msgs {
            inbox.push_message(msg);
        }

        let mut engine = WorkerEngine::<
            ConsumerWorker,
            WorkerEngineIO,
            WorkerInboxImpl,
            TestMetrics,
            PostgresConnection,
        >::new(worker_engine_config, manager, engine_io, inbox.clone());

        engine.try_init().await;

        engine.run().await;

        Ok(EngineOutcome {
            counters: engine.counters.clone(),
            leases: engine.leases.clone(),
            slots: engine.slots.clone(),
            capacity: engine.capacity.clone(),
            ready_slots: engine.ready_slots.clone(),
            waiters: engine.waiters.iter().map(|lease| lease.clone()).collect(),
        })
    }

    pub async fn run_with_custom_config<'a>(
        msgs: Vec<EngineMessage<ConsumerWorker>>,
        worker_engine_config: WorkerEngineConfig,
    ) -> Result<EngineOutcome, IOError> {
        let config = PostgresConfig::default();
        let manager = Arc::from(PostgresConnection::start(config));

        let _ = manager.drop_templates_like().await;

        let inbox_buffer: Arc<std::sync::Mutex<VecDeque<EngineMessage<ConsumerWorker>>>> =
            Arc::from(Mutex::new(VecDeque::new()));

        let inbox = WorkerInboxImpl::new(inbox_buffer.clone());
        let manager_to_bg = manager.clone();
        let engine_io =
            WorkerEngineIO::new(inbox_buffer.clone(), &manager_to_bg.template_database_name);

        for msg in msgs {
            inbox.push_message(msg);
        }

        let mut engine = WorkerEngine::<
            ConsumerWorker,
            WorkerEngineIO,
            WorkerInboxImpl,
            TestMetrics,
            PostgresConnection,
        >::new(worker_engine_config, manager, engine_io, inbox.clone());

        engine.try_init().await;

        engine.run().await;

        Ok(EngineOutcome {
            counters: engine.counters.clone(),
            leases: engine.leases.clone(),
            slots: engine.slots.clone(),
            capacity: engine.capacity.clone(),
            ready_slots: engine.ready_slots.clone(),
            waiters: engine.waiters.iter().map(|lease| lease.clone()).collect(),
        })
    }
}

pub fn db(n: usize) -> ReadString {
    ReadString::from(format!("pgtest_{n}"))
}

pub fn past_instant() -> std::time::Instant {
    std::time::Instant::now() - std::time::Duration::from_millis(30_001)
}

#[derive(Clone)]
pub struct ScriptedWorkerIO {
    script: Arc<Mutex<VecDeque<Result<(), IOError>>>>,
    inbox: Arc<Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
}

impl ScriptedWorkerIO {
    fn new(
        returns: Vec<Result<(), IOError>>,
        inbox: Arc<Mutex<VecDeque<EngineMessage<ConsumerWorker>>>>,
    ) -> Self {
        Self { script: Arc::new(Mutex::new(VecDeque::from(returns))), inbox }
    }

    pub(super) fn remaining(&self) -> usize {
        self.script.lock().unwrap().len()
    }
}

impl EngineIO<ConsumerWorker, PostgresConnection> for ScriptedWorkerIO {
    fn spawn_create_database(
        &self,
        worker_index: usize,
        _postgres_client: Arc<PostgresConnection>,
    ) -> Result<(), IOError> {
        let result = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .expect("ScriptedWorkerIO: spawn_create_database called more times than scripted");

        if result.is_ok() {
            self.inbox.lock().unwrap().push_back(EngineMessage::TemplateCreated {
                index: worker_index,
                result: Ok(ReadString::from(format!("grow_{worker_index}"))),
            });
        }

        result
    }

    fn spawn_recreate_database(
        &self,
        worker_index: usize,
        _database_name: ReadString,
        lease: LeaseId,
        generation: u64,
        _postgres_client: Arc<PostgresConnection>,
    ) -> Result<(), IOError> {
        let mut inbox = self.inbox.lock().unwrap();
        inbox.push_back(EngineMessage::DeleteLease { lease: lease.clone(), generation });
        inbox.push_back(EngineMessage::TemplateCreated {
            index: worker_index,
            result: Ok(ReadString::from(format!("grow_{worker_index}"))),
        });

        Ok(())
    }

    fn send_delayed_message(
        &self,
        _msg: EngineMessage<ConsumerWorker>,
        _wait_duration: u32,
        _cancel_token: CancellationToken,
    ) -> Result<(), IOError> {
        // TODO(user): timer semantics undecided — deliberately inert for now.
        Ok(())
    }

    fn send_message(&self, msg: EngineMessage<ConsumerWorker>) -> Result<(), IOError> {
        self.inbox.lock().unwrap().push_back(msg);
        Ok(())
    }
}

pub type GrowWorker = WorkerEngine<
    ConsumerWorker,
    ScriptedWorkerIO,
    WorkerInboxImpl,
    TestMetrics,
    PostgresConnection,
>;

pub fn grow_config() -> WorkerEngineConfig {
    WorkerEngineConfig {
        initial_slots: 1,
        maximum_slots: 2,
        starvation_threshold: 1,
        grow_batch_size: 1,
        ..WorkerEngineConfig::default()
    }
}

pub async fn run_grow(script: Vec<Result<(), IOError>>) -> (GrowWorker, ScriptedWorkerIO) {
    run_grow_with(grow_config(), script).await
}

pub async fn run_grow_with(
    config: WorkerEngineConfig,
    script: Vec<Result<(), IOError>>,
) -> (GrowWorker, ScriptedWorkerIO) {
    let manager = Arc::new(PostgresConnection::start(PostgresConfig::default()));

    let inbox_buffer: Arc<Mutex<VecDeque<EngineMessage<ConsumerWorker>>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    let inbox = WorkerInboxImpl::new(inbox_buffer.clone());
    let engine_io = ScriptedWorkerIO::new(script, inbox_buffer);

    let mut worker = GrowWorker::new(config, manager, engine_io.clone(), inbox);

    worker.try_init().await;
    worker.grow();
    worker.run().await;

    (worker, engine_io)
}
