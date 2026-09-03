use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use envconfig::Envconfig;
use tokio::sync::oneshot;

use crate::postgres_manager::{PostgresClientError, PostgresManager};

#[derive(Envconfig, Debug, Clone, Copy)]
struct PoolWorkerConfig {
    #[envconfig(from = "PGTEST_POOL_INITIAL_SIZE", default = "16")]
    pub pool_initial_size: u16,
    #[envconfig(from = "PGTEST_POOL_MAXIMUM_SIZE", default = "96")]
    pub pool_maximum_size: u16,
    #[envconfig(from = "PGTEST_POOL_STARVATION_THRESHOLD", default = "8")]
    pub pool_starvation_threshold: u16,
    #[envconfig(from = "PGTEST_POOL_GROW_BATCH_SIZE", default = "16")]
    pub grow_batch_size: u16,

    #[envconfig(from = "PGTEST_LEASE_GRACE_MS", default = "500")]
    pub lease_grace_ms: u32,
    #[envconfig(from = "PGTEST_LEASE_GRACE_MS", default = "30000")]
    pub lease_claim_timeout_ms: u32,
    #[envconfig(from = "PGTEST_POOL_DDL_CONCURRENCY", default = "5")]
    pub pool_ddl_concurrency: u32,
}

// Create struct
pub type LeaseId = String;

#[derive(Clone, Debug)]
enum Slot {
    Empty,
    Creating { db_name: String },
    Ready { db_name: String },
    Leased { db_name: String, lease: LeaseId },
    Done { db_name: String },
}

impl Slot {
    fn is_ready(&self) -> bool {
        matches!(self, Slot::Ready { db_name: _ })
    }

    fn db_name(&self) -> Option<String> {
        match self {
            Slot::Empty => None,
            Slot::Creating { db_name }
            | Slot::Done { db_name }
            | Slot::Leased { db_name, lease: _ }
            | Slot::Ready { db_name } => Some(db_name.clone()),
        }
    }
}

pub type SlotIdx = usize;

#[derive(Clone, Debug)]
struct LeaseEntry {
    slot_idx: SlotIdx,
    conns: u16,
    grace_epoch: u64, // TO be change for cancel token tokyo_util
}

// Esto no es para reclicar ready, sino para añadir mas "workers"
#[derive(Clone, Debug)]
struct Pool {
    maximum: u16,
    current: u16,
    free_workers: u16, // Change for VecDeque
    starvation_threshold: u16,
    growing: bool,
}

impl Pool {
    pub fn new(pool_config: PoolWorkerConfig) -> Pool {
        Pool {
            maximum: pool_config.pool_maximum_size,
            current: pool_config.pool_initial_size,
            free_workers: pool_config.pool_initial_size,
            starvation_threshold: pool_config.pool_starvation_threshold,
            growing: false,
        }
    }

    pub fn is_starving(&self) -> bool {
        self.starvation_threshold >= self.free_workers
    }

    pub fn is_growing(&self) -> bool {
        self.growing
    }

    pub fn is_at_maximum_capacity(&self) -> bool {
        self.maximum == self.current
    }

    pub fn set_pool_is_growing(&mut self) {
        self.growing = true;
    }

    pub fn leased_worker(&mut self) {
        self.free_workers -= 1;
    }

    pub fn grow_pool(&mut self, batch_size: u16) {
        self.current += batch_size;
        self.free_workers += batch_size;
        self.growing = false;
    }
}

#[derive(Clone)]
struct PoolWorker {
    workers: Box<[Slot]>,
    leases: HashMap<LeaseId, LeaseEntry>,
    pool: Pool,
    config: PoolWorkerConfig,
    pg_client: Arc<PostgresManager>,
    ready_workers: VecDeque<SlotIdx>,
}

pub enum WorkerMessage {
    AttachOrJoin { lease: LeaseId, reply: oneshot::Sender<WorkerResult> },
    TemplateCreated { index: SlotIdx, result: Result<String, PostgresClientError> },
    Shutdown,
    Metrics { reply: oneshot::Sender<WorkerResult> },
}

#[derive(Clone)]
pub enum WorkerResult {
    Attached { database_name: String },
    MetricsTest { workers: Box<[Slot]>, leases: HashMap<LeaseId, LeaseEntry>, pool: Pool },
}

impl PoolWorker {
    pub fn new(
        pool_worker_config: PoolWorkerConfig,
        postgres_manager: PostgresManager,
    ) -> PoolWorker {
        let workers: Box<[Slot]> =
            vec![Slot::Empty; pool_worker_config.pool_maximum_size as usize].into_boxed_slice();
        let pool = Pool::new(pool_worker_config);
        let leases = HashMap::new();
        let ready_workers: VecDeque<SlotIdx> = VecDeque::new();

        PoolWorker {
            workers,
            leases,
            pool,
            config: pool_worker_config.clone(), // TODO: Do we need all the config?
            pg_client: Arc::new(postgres_manager),
            ready_workers,
        }
    }

    pub async fn init(&mut self) {
        for index in 0..self.pool.current {
            match self.pg_client.create_database().await {
                Ok(template_name) => {
                    if let Some(elem) = self.workers.get_mut(index as usize) {
                        *elem = Slot::Ready { db_name: template_name.clone() };
                    }

                    self.ready_workers.push_back(index as usize);
                }
                Err(_) => {
                    // TODO handle the error possibles
                    continue;
                }
            };
        }
    }

    pub async fn run(
        &mut self,
        // Esto es la comunicación con el worker
        mut worker_rx: tokio::sync::mpsc::UnboundedReceiver<WorkerMessage>,
        // Para reenviarse mensajes
        worker_tx: tokio::sync::mpsc::UnboundedSender<WorkerMessage>,
    ) {
        let mut waiters: VecDeque<(LeaseId, oneshot::Sender<WorkerResult>)> = VecDeque::new();

        while let Some(msg) = worker_rx.recv().await {
            match msg {
                WorkerMessage::AttachOrJoin { lease, reply } => match self.leases.get_mut(&lease) {
                    Some(lease) => {
                        lease.conns += 1;
                        // TODO handle none, it means abort the connection
                        let worker_state = self.workers.get(lease.slot_idx).unwrap();

                        match worker_state {
                            Slot::Leased { db_name, lease: _ } => {
                                reply.send(WorkerResult::Attached {
                                    database_name: db_name.clone(),
                                });
                            }
                            _ => {
                                // Invariant
                                continue;
                            }
                        }
                    }
                    None => {
                        let Some(worker_index) = self.ready_workers.pop_front() else {
                            waiters.push_back((lease, reply));
                            return;
                        };

                        let database_name = self.lease_worker(worker_index, &lease);
                        self.grow(&worker_tx);
                        reply.send(WorkerResult::Attached { database_name: database_name.clone() });
                    }
                },
                WorkerMessage::TemplateCreated { index, result } => {
                    let Ok(template_name) = result else {
                        println!("Unable to create database, pending show error");
                        continue;
                    };
                    println!("Template created at {index} with name {template_name}")
                }
                WorkerMessage::Shutdown => {
                    return;
                }
                WorkerMessage::Metrics { reply } => {
                    reply.send(WorkerResult::MetricsTest {
                        workers: self.workers.clone(),
                        leases: self.leases.clone(),
                        pool: self.pool.clone(),
                    });
                }
            }
        }
    }

    fn lease_worker(&mut self, worker_index: SlotIdx, lease: &LeaseId) -> String {
        let Some(worker_state) = self.workers.get_mut(worker_index) else {
            panic!("Invariant: binding an empty slot")
        };
        if !worker_state.is_ready() {
            panic!("only ready workers con be leased")
        }
        let Some(database_name) = worker_state.db_name() else {
            panic!("We can't init a worker without database")
        };

        let new_state = Slot::Leased { db_name: database_name.clone(), lease: lease.clone() };
        *worker_state = new_state;
        self.pool.leased_worker();
        self.leases
            .insert(lease.clone(), LeaseEntry { slot_idx: worker_index, conns: 1, grace_epoch: 1 });

        database_name
    }

    fn grow(&mut self, worker_tx: &tokio::sync::mpsc::UnboundedSender<WorkerMessage>) {
        if self.pool.is_at_maximum_capacity() {
            return;
        }

        if self.pool.is_growing() {
            return;
        }

        if !self.pool.is_starving() {
            return;
        }

        self.pool.set_pool_is_growing();

        let batch_size = self.config.grow_batch_size.min(self.pool.maximum - self.pool.current);

        for idx in (self.pool.current - 1)..(self.pool.current + batch_size) {
            let mut free_slot = self.workers.get_mut(idx as usize);

            match free_slot {
                Some(_) => {
                    // TODO debug this is a error
                    continue;
                }
                None => {
                    free_slot = Some(&mut Slot::Creating { db_name: "random_name".to_string() });
                    let pg_client = self.pg_client.clone();
                    let producer_spawn = worker_tx.clone();
                    tokio::spawn(async move {
                        match pg_client.create_database().await {
                            Ok(template_name) => {
                                producer_spawn.send(WorkerMessage::TemplateCreated {
                                    index: idx as usize,
                                    result: Ok(template_name.clone()),
                                });
                            }
                            Err(error) => {
                                println!("TODO log errro")
                            }
                        }
                    });
                }
            }
        }

        self.pool.grow_pool(batch_size);
    }

    fn recycle(&mut self) {}

    #[cfg(test)]
    pub fn snapshot<'a>(&'a self) -> &'a Self {
        self
    }
}

// TODO use testcontainers to test this.
// TODO better test naming
#[cfg(test)]
mod pool_worker_test {

    use tokio::time::Instant;

    use crate::{
        pool_worker::{PoolWorker, PoolWorkerConfig, Slot, WorkerMessage, WorkerResult},
        postgres_manager::{PostgresConfig, PostgresManager},
    };

    impl Default for PostgresConfig {
        fn default() -> Self {
            Self {
                pgtest_pg_database: String::from("pgtest"),
                pgtest_pg_port: 5432,
                pgtest_pg_host: String::from("localhost"),
            }
        }
    }

    impl Default for PoolWorkerConfig {
        fn default() -> Self {
            Self {
                pool_initial_size: 4,
                pool_maximum_size: 12,
                pool_starvation_threshold: 2,
                grow_batch_size: 4,

                lease_grace_ms: 500,
                lease_claim_timeout_ms: 30_000,
                pool_ddl_concurrency: 5, // This is pool connection
            }
        }
    }

    #[tokio::test]
    async fn start_ok() {
        let config = PostgresConfig::default();

        let manager = PostgresManager::start(&config).await.unwrap();

        let pool_worker_config = PoolWorkerConfig::default();

        let mut pool_worker = PoolWorker::new(pool_worker_config, manager);

        let now_create = Instant::now();
        pool_worker.init().await;
        println!("Creating time {:.2?}", now_create.elapsed());

        let snapshot_pool = pool_worker.snapshot();

        assert_eq!(4, snapshot_pool.ready_workers.len());
    }

    #[tokio::test]
    async fn immediate_available_templates() {
        let config = PostgresConfig::default();

        let manager = PostgresManager::start(&config).await.unwrap();

        manager.drop_templates_like().await.unwrap();

        let pool_worker_config = PoolWorkerConfig::default();

        let mut pool_worker = PoolWorker::new(pool_worker_config, manager);

        pool_worker.init().await;

        let (worker_tx, worker_rx) = tokio::sync::mpsc::unbounded_channel();

        let worker_run_tx = worker_tx.clone();

        tokio::spawn(async move { pool_worker.run(worker_rx, worker_run_tx).await });

        let (attach_tx, attach_rx): (
            tokio::sync::oneshot::Sender<WorkerResult>,
            tokio::sync::oneshot::Receiver<WorkerResult>,
        ) = tokio::sync::oneshot::channel();

        let connection_name = "connection1".to_string();

        worker_tx
            .send(WorkerMessage::AttachOrJoin { lease: connection_name.clone(), reply: attach_tx })
            .unwrap();

        let WorkerResult::Attached { database_name: attached_database_name } =
            attach_rx.await.unwrap()
        else {
            panic!("Unexpected response");
        };

        let (metrics_tx, metrics_rx): (
            tokio::sync::oneshot::Sender<WorkerResult>,
            tokio::sync::oneshot::Receiver<WorkerResult>,
        ) = tokio::sync::oneshot::channel();

        worker_tx.send(WorkerMessage::Metrics { reply: metrics_tx }).unwrap();

        let WorkerResult::MetricsTest { workers, leases, pool } = metrics_rx.await.unwrap() else {
            panic!("Unexpected response");
        };

        let worker_lease_information = leases.get(&connection_name).unwrap();
        let Slot::Leased { db_name, lease } =
            workers.get(worker_lease_information.slot_idx).unwrap()
        else {
            panic!("Invariant state");
        };

        assert_eq!(*db_name, attached_database_name);
        assert_eq!(*lease, connection_name);
        assert_eq!(pool.free_workers, 3);
    }

    #[tokio::test]
    async fn starvation_templates() {
        let config = PostgresConfig::default();

        let manager = PostgresManager::start(&config).await.unwrap();

        manager.drop_templates_like().await.unwrap();

        let pool_worker_config = PoolWorkerConfig::default();

        let mut pool_worker = PoolWorker::new(pool_worker_config, manager);

        pool_worker.init().await;

        let (worker_tx, worker_rx) = tokio::sync::mpsc::unbounded_channel();

        let worker_run_tx = worker_tx.clone();

        tokio::spawn(async move { pool_worker.run(worker_rx, worker_run_tx).await });

        {
            let (attach_tx, attach_rx): (
                tokio::sync::oneshot::Sender<WorkerResult>,
                tokio::sync::oneshot::Receiver<WorkerResult>,
            ) = tokio::sync::oneshot::channel();

            let connection_name = "connection1".to_string();

            worker_tx
                .send(WorkerMessage::AttachOrJoin {
                    lease: connection_name.clone(),
                    reply: attach_tx,
                })
                .unwrap();

            let WorkerResult::Attached { database_name: _ } = attach_rx.await.unwrap() else {
                panic!("Unexpected response");
            };
        }

        {
            let (attach_tx, attach_rx): (
                tokio::sync::oneshot::Sender<WorkerResult>,
                tokio::sync::oneshot::Receiver<WorkerResult>,
            ) = tokio::sync::oneshot::channel();

            let connection_name = "connection2".to_string();

            worker_tx
                .send(WorkerMessage::AttachOrJoin {
                    lease: connection_name.clone(),
                    reply: attach_tx,
                })
                .unwrap();

            let WorkerResult::Attached { database_name: _ } = attach_rx.await.unwrap() else {
                panic!("Unexpected response");
            };
        }

        let (attach_tx, attach_rx): (
            tokio::sync::oneshot::Sender<WorkerResult>,
            tokio::sync::oneshot::Receiver<WorkerResult>,
        ) = tokio::sync::oneshot::channel();

        let connection_name = "connection3".to_string();

        worker_tx
            .send(WorkerMessage::AttachOrJoin { lease: connection_name.clone(), reply: attach_tx })
            .unwrap();

        let WorkerResult::Attached { database_name: attached_database_name } =
            attach_rx.await.unwrap()
        else {
            panic!("Unexpected response");
        };

        let mut current_size = 0;

        while current_size < 5 {
            let (metrics_tx, metrics_rx): (
                tokio::sync::oneshot::Sender<WorkerResult>,
                tokio::sync::oneshot::Receiver<WorkerResult>,
            ) = tokio::sync::oneshot::channel();

            worker_tx.send(WorkerMessage::Metrics { reply: metrics_tx }).unwrap();

            let WorkerResult::MetricsTest { workers, leases, pool } = metrics_rx.await.unwrap()
            else {
                panic!("Unexpected response");
            };

            println!("workers {:?}", workers);
            println!("leases {:?}", leases);
            println!("pool {:?}", pool);

            let worker_lease_information = leases.get(&connection_name).unwrap();
            let Slot::Leased { db_name, lease } =
                workers.get(worker_lease_information.slot_idx).unwrap()
            else {
                panic!("Invariant state");
            };

            assert_eq!(*db_name, attached_database_name);
            assert_eq!(*lease, connection_name);
            current_size = pool.free_workers;
        }

        assert_eq!(current_size, 5);
    }

    #[tokio::test]
    async fn join_previous_conn() {}

    #[tokio::test]
    async fn join_previous_conn_reset_timer() {}

    #[tokio::test]
    async fn non_waiting_grace() {}

    #[tokio::test]
    async fn waiting_grace() {}

    #[tokio::test]
    async fn max_lifetime_lease() {}

    #[tokio::test]
    async fn wait_for_ready_worker() {}

    #[tokio::test]
    async fn error_timeout_ready_worker() {}
}
