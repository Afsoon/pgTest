use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use hotpath::wrap::tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::timeout_at;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    postgres_manager::{PostgresClientError, PostgresConfig, PostgresManager},
    utils::ReadString,
    worker_engine::{
        core::{LeaseId, WorkerEngine, WorkerEngineConfig, is_valid_lease_id},
        database_jobs::{CleanupDatabase, CreateDatabase},
        errors::{AttachError, IOError, ReleaseError},
        messages::{ConsumerReply, EngineMessage},
        traits::{ConsumerIO, EngineIO, EngineInbox, MetricIO},
    },
    worker_manager::{
        database_cleanup_worker::DatabaseCleanupWorker,
        database_creation_worker::DatabaseCreationWorker,
    },
};

mod database_cleanup_worker;
mod database_creation_worker;

#[cfg(test)]
mod cleanup_tests;

#[cfg(test)]
mod reply_tests;

enum ManagerReply {
    Attached(LeaseSession),
    Engine(ConsumerReply),
}

struct ConsumerWorker {
    oneshot_channel: tokio::sync::oneshot::Sender<ManagerReply>,
    attachment: Option<(LeaseId, UnboundedSender<EngineMessage<ConsumerWorker>>)>,
}

impl ConsumerWorker {
    fn new(sender: tokio::sync::oneshot::Sender<ManagerReply>) -> Self {
        Self { oneshot_channel: sender, attachment: None }
    }
}

impl ConsumerIO for ConsumerWorker {
    fn reply(self, msg: ConsumerReply) -> Result<(), ConsumerReply> {
        if let (
            Some((lease, engine_tx)),
            ConsumerReply::Attached { database_name, generation, cancellation },
        ) = (&self.attachment, &msg)
        {
            // Transfer the guard through the channel. Dropping an unread
            // successful reply must unregister the connection just
            // like dropping a live session.
            let session = LeaseSession::new(
                database_name.clone(),
                lease.clone(),
                *generation,
                cancellation.clone(),
                engine_tx.clone(),
            );
            self.oneshot_channel.send(ManagerReply::Attached(session)).map_err(|reply| {
                if let ManagerReply::Attached(mut session) = reply {
                    // The engine rolls back a failed delivery synchronously.
                    session.detach_on_drop = false;
                }
                msg
            })
        } else {
            self.oneshot_channel.send(ManagerReply::Engine(msg)).map_err(|reply| {
                let ManagerReply::Engine(msg) = reply else { unreachable!() };
                msg
            })
        }
    }
}

struct DatabaseWorkerSenders {
    creation_tx: UnboundedSender<CreateDatabase>,
    cleanup_tx: UnboundedSender<CleanupDatabase>,
}

impl DatabaseWorkerSenders {
    fn init_database_worker_channels()
    -> (Self, UnboundedReceiver<CreateDatabase>, UnboundedReceiver<CleanupDatabase>) {
        let (creation_tx, creation_rx) = hotpath::channel!(
            tokio::sync::mpsc::unbounded_channel::<CreateDatabase>(),
            label = "database-creation"
        );

        let (cleanup_tx, cleanup_rx) = hotpath::channel!(
            tokio::sync::mpsc::unbounded_channel::<CleanupDatabase>(),
            label = "database-cleanup"
        );

        (Self { creation_tx, cleanup_tx }, creation_rx, cleanup_rx)
    }
}

struct WorkerEngineIO {
    send_message: UnboundedSender<EngineMessage<ConsumerWorker>>,
    tracker: TaskTracker,
    shutdown_token: CancellationToken,
    database_worker_senders: DatabaseWorkerSenders,
}

impl WorkerEngineIO {
    fn new(
        send_message: UnboundedSender<EngineMessage<ConsumerWorker>>,
        tracker: TaskTracker,
        shutdown_token: CancellationToken,
        database_worker_senders: DatabaseWorkerSenders,
    ) -> Self {
        Self { send_message, tracker, shutdown_token, database_worker_senders }
    }

    /// Every background task races against the shutdown token: cancelling it
    /// ends all in-flight work at its next await point, so shutdown never
    /// waits out a long timer or a slow DDL.
    fn spawn_cancellable<F, T>(&self, fut: F)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let shutdown = self.shutdown_token.clone();
        self.tracker.spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::debug!("background task cancelled by shutdown");
                }
                _ = fut => {}
            }
        });
    }
}

impl EngineIO<ConsumerWorker, PostgresManager> for WorkerEngineIO {
    fn request_creation(&self, request: CreateDatabase) -> Result<(), IOError> {
        let Err(error) = self.database_worker_senders.creation_tx.send(request) else {
            return Ok(());
        };

        tracing::error!(%error, "unable to send a creation database request");
        Err(IOError::FailedToSendTheMessage)
    }

    fn request_cleanup(&self, request: CleanupDatabase) -> Result<(), IOError> {
        let Err(error) = self.database_worker_senders.cleanup_tx.send(request) else {
            return Ok(());
        };

        tracing::error!(%error, "unable to send a creation database request");
        Err(IOError::FailedToSendTheMessage)
    }

    fn send_delayed_message(
        &self,
        msg: EngineMessage<ConsumerWorker>,
        wait_duration: u32,
        cancel_token: CancellationToken,
    ) -> Result<(), IOError> {
        let timer_producer = self.send_message.clone();
        self.spawn_cancellable(async move {
            tokio::select! {
                _ = cancel_token.cancelled() => {

                }
                _ =  tokio::time::sleep_until(
                    tokio::time::Instant::now() + Duration::from_millis(wait_duration as u64),
                ) => match timer_producer.send(msg) {
                    Ok(_) => {},
                    Err(_) => {},
                }

            }
        });

        Ok(())
    }

    fn send_message(&self, msg: EngineMessage<ConsumerWorker>) -> Result<(), IOError> {
        match self.send_message.send(msg) {
            Ok(_) => Ok(()),
            Err(_) => Err(IOError::FailedToSendTheMessage),
        }
    }
}

struct WorkerEngineInbox {
    receive_message: UnboundedReceiver<EngineMessage<ConsumerWorker>>,
}

impl WorkerEngineInbox {
    fn new(receive_message: UnboundedReceiver<EngineMessage<ConsumerWorker>>) -> Self {
        Self { receive_message }
    }
}

impl EngineInbox<ConsumerWorker> for WorkerEngineInbox {
    fn wait_for_message(
        &mut self,
    ) -> impl Future<Output = Option<EngineMessage<ConsumerWorker>>> + Send {
        self.receive_message.recv()
    }
}

pub struct Metrics {}

impl MetricIO for Metrics {
    fn send_metric(
        &self,
        _metric_message: crate::worker_engine::messages::EngineMetricMessage,
    ) -> impl Future<Output = Result<(), crate::worker_engine::errors::MetricIOError>> + Send {
        std::future::ready(Ok(()))
    }
}

pub struct WorkerEngineManager {
    worker_inbox_tx: UnboundedSender<EngineMessage<ConsumerWorker>>,
    pub pg_client: Arc<PostgresManager>,
    lease_claim_timeout: u64,
    engine_handle: tokio::task::JoinHandle<WorkerEngineType>,
    tracker: TaskTracker,
    shutdown_token: CancellationToken,
}

type WorkerEngineType =
    WorkerEngine<ConsumerWorker, WorkerEngineIO, WorkerEngineInbox, Metrics, PostgresManager>;

pub struct LeaseSession {
    pub database_name: ReadString,
    pub lease_id: LeaseId,
    generation: u64,
    cancellation: CancellationToken,
    detach_on_drop: bool,
    engine_tx: UnboundedSender<EngineMessage<ConsumerWorker>>,
}

impl LeaseSession {
    fn new(
        database_name: ReadString,
        lease_id: LeaseId,
        generation: u64,
        cancellation: CancellationToken,
        engine_tx: UnboundedSender<EngineMessage<ConsumerWorker>>,
    ) -> Self {
        Self { database_name, lease_id, generation, cancellation, detach_on_drop: true, engine_tx }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

impl Drop for LeaseSession {
    fn drop(&mut self) {
        if self.detach_on_drop {
            let _ = self.engine_tx.send(EngineMessage::Detach {
                lease: self.lease_id.clone(),
                generation: self.generation,
            });
        }
    }
}

impl WorkerEngineManager {
    #[hotpath::measure]
    pub async fn start(
        postgres_config: PostgresConfig,
        worker_engine_config: WorkerEngineConfig,
    ) -> Result<Self, ()> {
        if worker_engine_config.max_lease_records == 0 {
            tracing::error!("lease record capacity must be greater than zero");
            return Err(());
        }

        let postgres_client = match PostgresManager::start(postgres_config).await {
            Ok(pg_client) => Arc::new(pg_client),
            Err(error @ PostgresClientError::InvalidPoolSize(_)) => {
                tracing::error!(%error, "invalid PostgreSQL pool configuration");
                return Err(());
            }
            Err(PostgresClientError::DatabaseDoesNotExist(database_name)) => {
                tracing::error!(
                    "pgTest couldn't find {database_name} database to be used as template"
                );
                return Err(());
            }
            Err(PostgresClientError::UnsupportedVersion(version)) => {
                tracing::error!(
                    "The minimal version supported by pgTest is PostgreSQL 13; Detected \
                     PostgresSQL {version}"
                );
                return Err(());
            }
            Err(PostgresClientError::UnableToFetchPostgresVersion) => {
                tracing::error!(
                    "pgTest failed to query the Postgres version; Query used \"SELECT \
                     current_setting('server_version_num')::int8\" "
                );
                return Err(());
            }
            Err(PostgresClientError::UnableToFetchDatabaseList) => {
                tracing::error!(
                    "pgTest failed to query all databases created in Postgres; Query used \
                     \"SELECT datname from pg_database WHERE datname LIKE $1\" "
                );
                return Err(());
            }
            Err(PostgresClientError::UnableToConnectToPostgres(connection_string)) => {
                tracing::error!(
                    "Unable to connect using this connection string \"{connection_string}\" "
                );
                return Err(());
            }
            Err(PostgresClientError::UnexpectedServerVersionFormatFetched(server_version)) => {
                tracing::error!(
                    "The version query by pgTest have an unexpected format. The value obtained is \
                     {server_version}. Please report this as a bug"
                );
                return Err(());
            }
        };

        // Keep the large, instrumented startup futures out of this future's
        // inline state; nested profiling wrappers otherwise overflow the stack.
        let _ = Box::pin(postgres_client.drop_ddl_templates_like()).await;

        let (inbox_tx, inbox_rx) =
            hotpath::channel!(tokio::sync::mpsc::unbounded_channel(), label = "worker-inbox");

        let timeout_claim = worker_engine_config.lease_claim_timeout_ms.clone();

        let tracker = TaskTracker::new();
        let shutdown_token = CancellationToken::new();

        let (database_worker_senders, creation_rx, cleanup_rx) =
            DatabaseWorkerSenders::init_database_worker_channels();

        let creation_worker = DatabaseCreationWorker::new(
            inbox_tx.clone(),
            tracker.clone(),
            shutdown_token.clone(),
            postgres_client.clone(),
            creation_rx,
        );

        let cleanup_worker = DatabaseCleanupWorker::new(
            inbox_tx.clone(),
            tracker.clone(),
            shutdown_token.clone(),
            postgres_client.clone(),
            cleanup_rx,
        );

        let worker_engine_inbox = WorkerEngineInbox::new(inbox_rx);

        let worker_engine_io = WorkerEngineIO::new(
            inbox_tx.clone(),
            tracker.clone(),
            shutdown_token.clone(),
            database_worker_senders,
        );

        let mut worker_engine: WorkerEngineType = WorkerEngine::new(
            worker_engine_config,
            postgres_client.clone(),
            worker_engine_io,
            worker_engine_inbox,
        );

        Box::pin(worker_engine.try_init()).await;

        tracker.spawn(creation_worker.run());
        tracker.spawn(cleanup_worker.run());

        let engine_handle = tokio::spawn(async move {
            worker_engine.run().await;
            worker_engine
        });

        Ok(Self {
            worker_inbox_tx: inbox_tx.clone(),
            pg_client: postgres_client,
            lease_claim_timeout: timeout_claim,
            engine_handle,
            tracker,
            shutdown_token,
        })
    }

    #[hotpath::measure]
    pub async fn attach(
        &self,
        database_name: &str,
        lease: LeaseId,
    ) -> Result<LeaseSession, AttachError> {
        if !is_valid_lease_id(&lease) {
            return Err(AttachError::InvalidLeaseId);
        }
        let template_name = self.pg_client.template_database_name.template_name();
        if template_name.ne(database_name) {
            tracing::warn!(
                %lease,
                requested_database = database_name,
                expected_database = template_name,
                "Lease request rejected: database does not match the configured template"
            );
            return Err(AttachError::TemplateMismatch);
        }

        let now = tokio::time::Instant::now();
        let waiting_response_until = now + Duration::from_millis(self.lease_claim_timeout);
        let (reply_tx, reply_rx) =
            hotpath::channel!(tokio::sync::oneshot::channel(), proxy = true, label = "lease-reply");
        let consumer_worker = ConsumerWorker {
            oneshot_channel: reply_tx,
            attachment: Some((lease.clone(), self.worker_inbox_tx.clone())),
        };

        let request_database_msg = EngineMessage::AttachOrJoin {
            lease: lease.clone(),
            reply: consumer_worker,
            message_time: Instant::from(now),
        };
        let ask_worker_tx = self.worker_inbox_tx.clone();

        match ask_worker_tx.send(request_database_msg) {
            Ok(()) => {
                tracing::debug!(%lease, database_name, "Sent lease request to worker engine");
            }
            Err(_error) => {
                tracing::error!("Unable to ask for a lease");
                return Err(AttachError::EngineUnavailable);
            }
        }

        let reply = if self.lease_claim_timeout == 0 {
            reply_rx.await
        } else {
            timeout_at(waiting_response_until, reply_rx).await.map_err(|_| AttachError::TimedOut)?
        }
        .map_err(|_| AttachError::EngineUnavailable)?;
        match reply {
            ManagerReply::Attached(session) => {
                if session.cancellation.is_cancelled() {
                    return Err(AttachError::LeaseClosed);
                }
                Ok(session)
            }
            ManagerReply::Engine(ConsumerReply::AttachRejected(error)) => Err(error),
            _ => Err(AttachError::Failed),
        }
    }

    /// Success acknowledges logical closure; physical deletion runs in the
    /// background.
    #[hotpath::measure]
    pub async fn release(&self, lease: LeaseId) -> Result<(), ReleaseError> {
        if !is_valid_lease_id(&lease) {
            return Err(ReleaseError::InvalidLeaseId);
        }
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.worker_inbox_tx
            .send(EngineMessage::ReleaseLease { lease, reply: ConsumerWorker::new(reply_tx) })
            .map_err(|_| ReleaseError::EngineUnavailable)?;
        let reply = tokio::time::timeout(Duration::from_secs(5), reply_rx)
            .await
            .map_err(|_| ReleaseError::ReplyTimedOut)?
            .map_err(|_| ReleaseError::EngineUnavailable)?;
        match reply {
            ManagerReply::Engine(ConsumerReply::ReleaseResult(result)) => result,
            _ => Err(ReleaseError::UnexpectedReply),
        }
    }

    #[hotpath::measure]
    pub async fn shutdown(self) {
        self.shutdown_token.cancel();
        self.tracker.close();
        self.tracker.wait().await;

        let _ = self.worker_inbox_tx.send(EngineMessage::Shutdown);
        let _ = self.engine_handle.await;
    }

    #[cfg(all(test, not(feature = "stable_ids")))]
    async fn sync_inbox(&self) {
        let (reply, received) = tokio::sync::oneshot::channel();
        assert!(
            self.worker_inbox_tx.send(EngineMessage::Barrier { reply }).is_ok(),
            "engine inbox must remain open"
        );
        received.await.expect("engine must acknowledge the inbox barrier");
    }

    #[cfg(all(test, not(feature = "stable_ids")))]
    async fn drain_and_snapshot(self) -> WorkerEngineType {
        self.sync_inbox().await;
        // Receiver loops now live in the tracker too. Cancel them before
        // waiting.
        self.shutdown_token.cancel();
        self.tracker.close();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                self.tracker.wait().await;
                self.sync_inbox().await;
                if self.tracker.is_empty() {
                    break;
                }
            }
        })
        .await
        .expect("background workers must stop");

        let _ = self.worker_inbox_tx.send(EngineMessage::Shutdown);
        self.engine_handle.await.expect("engine task panicked")
    }
}

#[cfg(all(test, not(feature = "stable_ids")))]
mod worker_engine_manager_test {
    use std::{future::poll_fn, pin::Pin, task::Poll, time::Duration};

    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    use crate::{
        postgres_manager::pg_container_config,
        utils::ReadString,
        worker_engine::{
            core::WorkerEngineConfig,
            messages::EngineMessage,
            traits::{EngineIO, PostgresClient},
        },
        worker_manager::{WorkerEngineIO, WorkerEngineManager},
    };

    fn no_growth_config(slots: u16) -> WorkerEngineConfig {
        WorkerEngineConfig {
            initial_slots: slots,
            grow_batch_size: 0,
            lease_claim_timeout_ms: 30_000,
            ..WorkerEngineConfig::default()
        }
    }

    #[test]
    fn startup_future_fits_stack_budget() {
        // Nested profiling wrappers used to inflate this to over 600 KiB,
        // overflowing the main thread's stack when startup was first polled.
        let startup = WorkerEngineManager::start(
            crate::postgres_manager::PostgresConfig::default(),
            WorkerEngineConfig::default(),
        );
        let size = std::mem::size_of_val(&startup);
        assert!(size < 64 * 1024, "startup future uses {size} bytes");
    }

    async fn start_manager(config: WorkerEngineConfig) -> WorkerEngineManager {
        WorkerEngineManager::start(pg_container_config().await, config)
            .await
            .expect("engine must start")
    }

    // Keep the real manager/engine and PostgreSQL, but explicitly deliver DDL
    // results so timeout and cancellation tests do not depend on database
    // creation speed.
    struct DeferredWorkers {
        creation_rx: super::UnboundedReceiver<super::CreateDatabase>,
        _cleanup_rx: super::UnboundedReceiver<super::CleanupDatabase>,
    }

    impl DeferredWorkers {
        async fn complete_creation(&mut self, manager: &WorkerEngineManager) -> ReadString {
            let request = tokio::time::timeout(Duration::from_secs(5), self.creation_rx.recv())
                .await
                .expect("creation must be queued")
                .expect("creation channel must be open");
            let database_name =
                manager.pg_client.create_database().await.expect("test database must be created");
            manager
                .worker_inbox_tx
                .send(EngineMessage::DatabaseWorker(
                    crate::worker_engine::database_jobs::DatabaseWorkerMessages::CreationFinished {
                        database_id: request.database_id,
                        result: Ok(database_name.clone()),
                    },
                ))
                .unwrap();
            manager.sync_inbox().await;
            database_name
        }
    }

    async fn start_with_deferred_workers() -> (WorkerEngineManager, DeferredWorkers) {
        let config = WorkerEngineConfig {
            initial_slots: 1,
            starvation_threshold: 0,
            grow_batch_size: 1,
            ..WorkerEngineConfig::default()
        };
        let pg_client = std::sync::Arc::new(
            crate::postgres_manager::PostgresManager::start(pg_container_config().await)
                .await
                .expect("PostgreSQL must start"),
        );
        let (engine_tx, engine_rx) = hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
        let tracker = tokio_util::task::TaskTracker::new();
        let shutdown_token = CancellationToken::new();
        let (senders, creation_rx, cleanup_rx) =
            super::DatabaseWorkerSenders::init_database_worker_channels();
        let io = WorkerEngineIO::new(
            engine_tx.clone(),
            tracker.clone(),
            shutdown_token.clone(),
            senders,
        );
        let mut engine = super::WorkerEngineType::new(
            config,
            pg_client.clone(),
            io,
            super::WorkerEngineInbox::new(engine_rx),
        );
        engine.try_init().await;
        let engine_handle = tokio::spawn(async move {
            engine.run().await;
            engine
        });
        let manager = WorkerEngineManager {
            worker_inbox_tx: engine_tx,
            pg_client,
            lease_claim_timeout: config.lease_claim_timeout_ms,
            engine_handle,
            tracker,
            shutdown_token,
        };
        (manager, DeferredWorkers { creation_rx, _cleanup_rx: cleanup_rx })
    }

    async fn enqueue_attach<F: Future>(manager: &WorkerEngineManager, mut attach: Pin<&mut F>) {
        poll_fn(|cx| {
            assert!(attach.as_mut().poll(cx).is_pending(), "attach must wait for a slot");
            Poll::Ready(())
        })
        .await;
        manager.sync_inbox().await;
    }

    #[tokio::test]
    async fn immediate_available_templates() {
        start_manager(WorkerEngineConfig::default()).await.shutdown().await;
    }

    #[tokio::test]
    async fn when_the_last_lease_session_is_dropped_then_the_database_remains_assigned() {
        let worker_engine_config =
            WorkerEngineConfig { lease_claim_timeout_ms: 5_000, ..WorkerEngineConfig::default() };
        let manager = start_manager(worker_engine_config).await;

        let lease = ReadString::from("connection1");
        let session = manager
            .attach("pgtest", lease.clone())
            .await
            .expect("attach against the template database succeeds");
        let assigned_database = session.database_name.to_string();

        drop(session);

        let engine = manager.drain_and_snapshot().await;
        let snapshot = engine.snapshot();

        assert_eq!(snapshot.leases.get(&lease).expect("lease must remain assigned").conns, 0);
        assert_eq!(snapshot.leases.len(), 1);
        assert_eq!(
            snapshot.inventory.ready.len(),
            usize::from(WorkerEngineConfig::default().initial_slots - 1)
        );

        let mut assigned_config = pg_container_config().await;
        assigned_config.pgtest_pg_database = assigned_database;
        assert!(
            crate::postgres_manager::PostgresManager::start(assigned_config).await.is_ok(),
            "the database must still exist after its last session closes"
        );
    }

    #[tokio::test]
    async fn given_a_lease_with_multiples_connection_when_one_session_is_dropped_then_lease_connection_is_not_tracked_anymore()
     {
        let worker_engine_config =
            WorkerEngineConfig { lease_claim_timeout_ms: 5_000, ..WorkerEngineConfig::default() };
        let manager = start_manager(worker_engine_config).await;

        let lease = ReadString::from("connection1");
        let _no_dropped_session = manager
            .attach("pgtest", lease.clone())
            .await
            .expect("attach against the template database succeeds");

        let session = manager
            .attach("pgtest", lease.clone())
            .await
            .expect("attach against the template database succeeds");

        drop(session);

        let engine = manager.drain_and_snapshot().await;
        let snapshot = engine.snapshot();

        assert!(snapshot.leases.contains_key(&lease));
        assert_eq!(snapshot.leases.len(), 1);
        assert_eq!(snapshot.leases[&lease].conns, 1);
        assert_eq!(
            snapshot.inventory.ready.len(),
            usize::from(WorkerEngineConfig::default().initial_slots - 1)
        );
    }

    #[tokio::test]
    async fn attach_unknown_template_rejected() {
        let manager = start_manager(WorkerEngineConfig::default()).await;

        let result = manager.attach("not_the_template", ReadString::from("connection1")).await;
        assert!(result.is_err());

        let engine = manager.drain_and_snapshot().await;
        assert!(engine.snapshot().leases.is_empty());
    }

    #[tokio::test]
    async fn when_attach_times_out_without_free_slots_then_the_consumer_stops_waiting_for_a_response()
     {
        let worker_engine_config = WorkerEngineConfig {
            initial_slots: 0,
            grow_batch_size: 0,
            lease_claim_timeout_ms: 100,
            ..WorkerEngineConfig::default()
        };

        let manager = start_manager(worker_engine_config).await;

        tokio::time::pause();
        let lease = ReadString::from("connection1");
        let started_at = tokio::time::Instant::now();
        let result = manager.attach("pgtest", lease.clone()).await;
        assert!(result.is_err(), "attach must time out when no slot can ever free up");
        assert!(
            started_at.elapsed()
                >= std::time::Duration::from_millis(worker_engine_config.lease_claim_timeout_ms),
            "attach must wait until the claim deadline"
        );

        tokio::time::resume();
        let engine = manager.drain_and_snapshot().await;
        let snapshot = engine.snapshot();
        assert!(snapshot.leases.is_empty());
        assert_eq!(
            snapshot.waiters.len(),
            1,
            "the timed-out claim remains queued until supply arrives"
        );
        assert_eq!(snapshot.waiters.front().unwrap(), &lease);
    }

    #[tokio::test]
    async fn creation_after_attach_timeout_leaves_the_new_database_ready() {
        let (mut manager, mut workers) = start_with_deferred_workers().await;
        let session = manager
            .attach("pgtest", ReadString::from("holder"))
            .await
            .expect("first lease must occupy the only slot");
        let original_database = session.database_name.clone();

        // The engine uses its claim setting for lease lifetime too. Shorten
        // only the caller's deadline, then explicitly deliver lifetime expiry.
        manager.lease_claim_timeout = 100;
        let started_at = Instant::now();
        let result = manager.attach("pgtest", ReadString::from("timed_out")).await;
        assert!(result.is_err(), "the occupied slot cannot satisfy the request");
        assert!(started_at.elapsed() >= Duration::from_millis(100));

        drop(session);
        manager
            .worker_inbox_tx
            .send(EngineMessage::LeaseMaxTimeReached {
                lease: ReadString::from("holder"),
                generation: 1,
            })
            .unwrap();
        let created = workers.complete_creation(&manager).await;
        let engine = manager.drain_and_snapshot().await;
        let snapshot = engine.snapshot();
        assert!(snapshot.leases.is_empty());
        assert!(snapshot.waiters.is_empty(), "creation must skip the closed reply channel");
        assert_eq!(snapshot.inventory.ready.len(), 1);
        assert_eq!(snapshot.inventory.ready[0].database_name, created);
        assert_ne!(created, original_database);
        assert_eq!(snapshot.inventory.retiring.len(), 1, "cleanup has not completed");
    }

    #[tokio::test]
    async fn queued_attach_receives_creation_while_cleanup_is_pending() {
        let (manager, mut workers) = start_with_deferred_workers().await;
        let holder = ReadString::from("holder");
        let waiting = ReadString::from("waiting");
        let session = manager
            .attach("pgtest", holder.clone())
            .await
            .expect("first lease must occupy the only slot");

        let mut attach = Box::pin(manager.attach("pgtest", waiting.clone()));
        enqueue_attach(&manager, attach.as_mut()).await;
        drop(session);
        manager
            .worker_inbox_tx
            .send(EngineMessage::LeaseMaxTimeReached { lease: holder.clone(), generation: 1 })
            .unwrap();
        let created = workers.complete_creation(&manager).await;
        let received_session = attach.await.expect("queued attach must receive the fresh database");
        assert_eq!(received_session.database_name, created);

        let engine = manager.drain_and_snapshot().await;
        let snapshot = engine.snapshot();
        assert!(snapshot.waiters.is_empty());
        assert!(!snapshot.leases.contains_key(&holder));
        assert_eq!(snapshot.leases.len(), 1);
        assert_eq!(snapshot.leases.get(&waiting).expect("new lease must exist").conns, 1);
        assert_eq!(
            snapshot.leases[&waiting].database.database_name,
            received_session.database_name
        );
        assert_eq!(snapshot.inventory.retiring.len(), 1, "attachment did not wait for cleanup");
        assert_eq!(snapshot.inventory.ready.len(), 0);
        drop(received_session);
    }

    #[tokio::test]
    async fn creation_skips_a_cancelled_attach_and_keeps_the_database_ready() {
        let (manager, mut workers) = start_with_deferred_workers().await;
        let session = manager
            .attach("pgtest", ReadString::from("holder"))
            .await
            .expect("first lease must occupy the only slot");
        let original_database = session.database_name.clone();

        let started_at = Instant::now();
        let mut attach = Box::pin(manager.attach("pgtest", ReadString::from("cancelled")));
        enqueue_attach(&manager, attach.as_mut()).await;
        drop(attach);
        drop(session);
        manager
            .worker_inbox_tx
            .send(EngineMessage::LeaseMaxTimeReached {
                lease: ReadString::from("holder"),
                generation: 1,
            })
            .unwrap();

        let created = workers.complete_creation(&manager).await;
        let engine = manager.drain_and_snapshot().await;
        assert!(
            started_at.elapsed() < Duration::from_secs(30),
            "the reply must be skipped because its receiver closed, before waiter expiry"
        );
        let snapshot = engine.snapshot();
        assert!(snapshot.leases.is_empty());
        assert!(snapshot.waiters.is_empty());
        assert_eq!(snapshot.inventory.ready[0].database_name, created);
        assert_ne!(created, original_database);
        assert_eq!(snapshot.inventory.ready.len(), 1);
    }

    #[tokio::test]
    async fn given_a_closed_engine_inbox_when_attaching_then_the_caller_fails_without_waiting() {
        let mut manager = start_manager(no_growth_config(0)).await;
        assert!(manager.worker_inbox_tx.send(EngineMessage::Shutdown).is_ok());
        let engine = (&mut manager.engine_handle).await.expect("engine must not panic");
        drop(engine);
        assert!(manager.worker_inbox_tx.is_closed());

        let result = manager.attach("pgtest", ReadString::from("after_shutdown")).await;

        manager.shutdown_token.cancel();
        manager.tracker.close();
        manager.tracker.wait().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn given_a_pending_delayed_message_when_shutting_down_then_the_task_is_cancelled() {
        let manager = start_manager(no_growth_config(0)).await;
        let tracker = manager.tracker.clone();
        let inbox = manager.worker_inbox_tx.clone();
        let initial_tasks = tracker.len();
        let (senders, _creation_rx, _cleanup_rx) =
            super::DatabaseWorkerSenders::init_database_worker_channels();
        let io = WorkerEngineIO::new(
            inbox.clone(),
            tracker.clone(),
            manager.shutdown_token.clone(),
            senders,
        );
        let (reply, mut received) = tokio::sync::oneshot::channel();
        io.send_delayed_message(EngineMessage::Barrier { reply }, 60_000, CancellationToken::new())
            .expect("delayed message task must be scheduled");

        assert_eq!(tracker.len(), initial_tasks + 1);
        assert!(matches!(
            received.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        manager.shutdown().await;
        assert!(tracker.is_empty());
        assert!(inbox.is_closed());
        assert!(received.await.is_err(), "shutdown must discard the delayed message");
    }
}
