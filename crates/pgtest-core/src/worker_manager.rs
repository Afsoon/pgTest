use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::time::timeout_at;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    postgres_manager::{PostgresClientError, PostgresConfig, PostgresManager},
    utils::ReadString,
    worker_engine::{
        core::{LeaseId, WorkerEngine, WorkerEngineConfig},
        errors::IOError,
        messages::{
            ConsumerReply::{self, Attached},
            EngineMessage,
        },
        traits::{ConsumerIO, EngineIO, EngineInbox, MetricIO, PostgresClient},
    },
};

struct ConsumerWorker {
    oneshot_channel: tokio::sync::oneshot::Sender<ConsumerReply>,
}

impl ConsumerWorker {
    pub fn new(sender_tx: tokio::sync::oneshot::Sender<ConsumerReply>) -> Self {
        Self { oneshot_channel: sender_tx }
    }
}

impl ConsumerIO for ConsumerWorker {
    fn reply(self, msg: ConsumerReply) -> Result<(), ConsumerReply> {
        self.oneshot_channel.send(msg)
    }
}

struct WorkerEngineIO {
    send_message: tokio::sync::mpsc::UnboundedSender<EngineMessage<ConsumerWorker>>,
    tracker: TaskTracker,
    shutdown_token: CancellationToken,
}

impl WorkerEngineIO {
    fn new(
        send_message: tokio::sync::mpsc::UnboundedSender<EngineMessage<ConsumerWorker>>,
        tracker: TaskTracker,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self { send_message, tracker, shutdown_token }
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
    fn spawn_create_database(
        &self,
        worker_index: usize,
        postgres_client: Arc<PostgresManager>,
    ) -> Result<(), IOError> {
        let producer_spawn = self.send_message.clone();

        self.spawn_cancellable(async move {
            match postgres_client.create_database().await {
                Ok(template_name) => {
                    match producer_spawn.send(EngineMessage::TemplateCreated {
                        index: worker_index as usize,
                        result: Ok(template_name.clone()),
                    }) {
                        Ok(_) => Ok(()),
                        Err(_) => Err(IOError::FailedToSendTheMessage),
                    }
                }
                Err(error) => {
                    match producer_spawn.send(EngineMessage::TemplateCreated {
                        index: worker_index as usize,
                        result: Err(error),
                    }) {
                        Ok(_) => Ok(()),
                        Err(_) => Err(IOError::FailedToSendTheMessage),
                    }
                }
            }
        });

        Ok(())
    }

    fn spawn_recreate_database(
        &self,
        worker_index: usize,
        database_name: ReadString,
        lease: LeaseId,
        postgres_client: Arc<PostgresManager>,
    ) -> Result<(), IOError> {
        let producer_spawn = self.send_message.clone();
        self.spawn_cancellable(async move {
            match postgres_client.drop_database(&database_name).await {
                Ok(_) => match producer_spawn.send(EngineMessage::DeleteLease { lease }) {
                    Ok(_) => {}
                    Err(_) => {
                        tracing::error!(
                            "Unable to communicate with the worker to delete a unused lease"
                        );
                        return;
                    }
                },
                Err(error) => {
                    tracing::error!("Unable to drop {database_name} in Postgres: Source {error}");
                    tracing::error!("Trying to create a new database");
                }
            };

            match postgres_client.create_database().await {
                Ok(template_name) => {
                    match producer_spawn.send(EngineMessage::TemplateCreated {
                        index: worker_index as usize,
                        result: Ok(template_name.clone()),
                    }) {
                        Ok(_) => {}
                        Err(_) => {
                            tracing::error!(
                                "Unable to create a new database after delete an used database"
                            );
                        }
                    }
                }
                Err(error) => {
                    match producer_spawn.send(EngineMessage::TemplateCreated {
                        index: worker_index as usize,
                        result: Err(error),
                    }) {
                        Ok(_) => {}
                        Err(_) => {
                            tracing::error!(
                                "Unable to communicate with the worker to indicate a new database \
                                 is ready"
                            );
                        }
                    }
                }
            }
        });

        Ok(())
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
    receive_message: tokio::sync::mpsc::UnboundedReceiver<EngineMessage<ConsumerWorker>>,
}

impl WorkerEngineInbox {
    fn new(
        receive_message: tokio::sync::mpsc::UnboundedReceiver<EngineMessage<ConsumerWorker>>,
    ) -> Self {
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
    worker_inbox_tx: tokio::sync::mpsc::UnboundedSender<EngineMessage<ConsumerWorker>>,
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
    engine_tx: tokio::sync::mpsc::UnboundedSender<EngineMessage<ConsumerWorker>>,
}

impl LeaseSession {
    fn new(
        database_name: ReadString,
        lease_id: LeaseId,
        engine_tx: tokio::sync::mpsc::UnboundedSender<EngineMessage<ConsumerWorker>>,
    ) -> Self {
        Self { database_name, lease_id, engine_tx }
    }
}

impl Drop for LeaseSession {
    fn drop(&mut self) {
        tracing::debug!("Sending the message to detach the lease");
        let _ = self.engine_tx.send(EngineMessage::Detach { lease: self.lease_id.clone() });
    }
}

impl WorkerEngineManager {
    pub async fn start(
        postgres_config: PostgresConfig,
        worker_engine_config: WorkerEngineConfig,
    ) -> Result<Self, ()> {
        let postgres_client = match PostgresManager::start(postgres_config).await {
            Ok(pg_client) => Arc::new(pg_client),
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

        let _ = postgres_client.drop_ddl_templates_like().await;

        let (inbox_tx, inbox_rx) = tokio::sync::mpsc::unbounded_channel();

        let timeout_claim = worker_engine_config.lease_claim_timeout_ms.clone();

        let tracker = TaskTracker::new();
        let shutdown_token = CancellationToken::new();
        let worker_engine_inbox = WorkerEngineInbox::new(inbox_rx);
        let worker_engine_io =
            WorkerEngineIO::new(inbox_tx.clone(), tracker.clone(), shutdown_token.clone());

        let mut worker_engine: WorkerEngineType = WorkerEngine::new(
            worker_engine_config,
            postgres_client.clone(),
            worker_engine_io,
            worker_engine_inbox,
        );

        worker_engine.try_init().await;

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

    pub async fn attach(&self, database_name: &str, lease: LeaseId) -> Result<LeaseSession, ()> {
        let template_name = self.pg_client.template_database_name.template_name();
        if template_name.ne(database_name) {
            tracing::warn!(
                %lease,
                requested_database = database_name,
                expected_database = template_name,
                "Lease request rejected: database does not match the configured template"
            );
            return Err(());
        }

        let now = tokio::time::Instant::now();
        let waiting_response_until = now + Duration::from_millis(self.lease_claim_timeout);
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let consumer_worker = ConsumerWorker::new(reply_tx);

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
                return Err(());
            }
        }

        match timeout_at(waiting_response_until, reply_rx).await {
            Err(_) => {
                tracing::error!("Worker was unable to answer on time");
                Err(())
            }
            Ok(reply) => {
                let Ok(Attached { database_name }) = reply else { return Err(()) };
                Ok(LeaseSession::new(database_name, lease, self.worker_inbox_tx.clone()))
            }
        }
    }

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
    pub(crate) async fn drain_and_snapshot(self, cancel_spawned_threads: bool) -> WorkerEngineType {
        if cancel_spawned_threads {
            self.shutdown_token.cancel()
        }
        self.sync_inbox().await;
        self.tracker.close();
        loop {
            self.tracker.wait().await;
            self.sync_inbox().await;
            if self.tracker.is_empty() {
                break;
            }
        }

        let _ = self.worker_inbox_tx.send(EngineMessage::Shutdown);
        self.engine_handle.await.expect("engine task panicked")
    }
}

#[cfg(all(test, not(feature = "stable_ids")))]
mod worker_engine_manager_test {
    use std::{future::poll_fn, pin::Pin, task::Poll, time::Duration};

    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;
    use tracing_test::traced_test;

    use crate::{
        postgres_manager::pg_container_config,
        utils::ReadString,
        worker_engine::{
            core::{Slot, WorkerEngineConfig},
            messages::EngineMessage,
            traits::EngineIO,
        },
        worker_manager::{WorkerEngineIO, WorkerEngineManager},
    };

    fn fixed_pool_config(slots: u16) -> WorkerEngineConfig {
        WorkerEngineConfig {
            initial_slots: slots,
            maximum_slots: slots,
            lease_claim_timeout_ms: 30_000,
            lease_grace_ms: 0,
            ..WorkerEngineConfig::default()
        }
    }

    async fn start_manager(config: WorkerEngineConfig) -> WorkerEngineManager {
        WorkerEngineManager::start(pg_container_config().await, config)
            .await
            .expect("engine must start")
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
        start_manager(WorkerEngineConfig::default()).await;
    }

    #[tokio::test]
    async fn when_a_lease_session_is_dropped_then_a_detach_message_is_send_to_signal_end_of_the_connection()
     {
        let worker_engine_config =
            WorkerEngineConfig { lease_claim_timeout_ms: 5_000, ..WorkerEngineConfig::default() };
        let manager = start_manager(worker_engine_config).await;

        let lease = ReadString::from("connection1");
        let session = manager
            .attach("pgtest", lease.clone())
            .await
            .expect("attach against the template database succeeds");

        drop(session);

        let engine = manager.drain_and_snapshot(false).await;
        let snapshot = engine.snapshot();

        assert!(!snapshot.leases.contains_key(&lease));
        let leased =
            snapshot.slots.iter().filter(|slot| matches!(slot, Slot::Leased { .. })).count();
        assert_eq!(leased, 0);
        assert_eq!(snapshot.capacity.free_slots, WorkerEngineConfig::default().initial_slots);
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

        let engine = manager.drain_and_snapshot(true).await;
        let snapshot = engine.snapshot();

        assert!(snapshot.leases.contains_key(&lease));
        let leased =
            snapshot.slots.iter().filter(|slot| matches!(slot, Slot::Leased { .. })).count();
        assert_eq!(leased, 1);
        assert_eq!(snapshot.capacity.free_slots, WorkerEngineConfig::default().initial_slots - 1);
    }

    #[tokio::test]
    async fn attach_unknown_template_rejected() {
        let manager = start_manager(WorkerEngineConfig::default()).await;

        let result = manager.attach("not_the_template", ReadString::from("connection1")).await;
        assert!(result.is_err());

        let engine = manager.drain_and_snapshot(false).await;
        assert!(engine.snapshot().leases.is_empty());
    }

    #[tokio::test]
    async fn when_attach_times_out_without_free_slots_then_the_consumer_stops_waiting_for_a_response()
     {
        let worker_engine_config = WorkerEngineConfig {
            initial_slots: 0,
            maximum_slots: 0,
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
        let engine = manager.drain_and_snapshot(false).await;
        let snapshot = engine.snapshot();
        assert!(snapshot.leases.is_empty());
        assert_eq!(snapshot.waiters.len(), 1, "the timed-out claim stays parked in the engine");
        assert_eq!(snapshot.waiters.front().unwrap().0, lease);
    }

    #[tokio::test]
    async fn given_a_full_pool_when_a_slot_is_recycled_after_attach_times_out_then_it_stays_ready()
    {
        let mut manager = start_manager(fixed_pool_config(1)).await;
        let session = manager
            .attach("pgtest", ReadString::from("holder"))
            .await
            .expect("first lease must occupy the only slot");
        let original_database = session.database_name.clone();

        // The engine uses its claim setting for lease lifetime too. Shorten
        // only the caller's deadline so recycling is triggered by session drop.
        manager.lease_claim_timeout = 100;
        let started_at = Instant::now();
        let result = manager.attach("pgtest", ReadString::from("timed_out")).await;
        assert!(result.is_err(), "the occupied slot cannot satisfy the request");
        assert!(started_at.elapsed() >= Duration::from_millis(100));

        drop(session);
        let engine = manager.drain_and_snapshot(false).await;
        let snapshot = engine.snapshot();
        assert!(snapshot.leases.is_empty());
        assert!(snapshot.waiters.is_empty(), "recycling must remove the expired caller");
        assert!(matches!(
            &snapshot.slots[..],
            [Slot::Ready { db_name }] if *db_name != original_database
        ));
        assert_eq!(snapshot.capacity.free_slots, 1);
    }

    #[tokio::test]
    async fn given_a_queued_attach_when_a_slot_is_recycled_then_the_caller_receives_the_database() {
        let manager = start_manager(fixed_pool_config(1)).await;
        let holder = ReadString::from("holder");
        let waiting = ReadString::from("waiting");
        let session = manager
            .attach("pgtest", holder.clone())
            .await
            .expect("first lease must occupy the only slot");

        let mut attach = Box::pin(manager.attach("pgtest", waiting.clone()));
        enqueue_attach(&manager, attach.as_mut()).await;
        drop(session);
        let received_session =
            attach.await.expect("queued attach must receive the recycled database");

        let engine = manager.drain_and_snapshot(true).await;
        let snapshot = engine.snapshot();
        assert!(snapshot.waiters.is_empty());
        assert!(!snapshot.leases.contains_key(&holder));
        assert_eq!(snapshot.leases.len(), 1);
        assert_eq!(snapshot.leases.get(&waiting).expect("new lease must exist").conns, 1);
        assert!(matches!(
            &snapshot.slots[..],
            [Slot::Leased { db_name, lease }]
                if *db_name == received_session.database_name && *lease == waiting
        ));
        assert_eq!(snapshot.capacity.free_slots, 0);
        drop(received_session);
    }

    #[tokio::test]
    async fn given_a_cancelled_attach_when_a_slot_is_recycled_then_the_closed_reply_is_skipped() {
        let manager = start_manager(fixed_pool_config(1)).await;
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

        let engine = manager.drain_and_snapshot(false).await;
        assert!(
            started_at.elapsed() < Duration::from_secs(30),
            "the reply must be skipped because its receiver closed, before waiter expiry"
        );
        let snapshot = engine.snapshot();
        assert!(snapshot.leases.is_empty());
        assert!(snapshot.waiters.is_empty());
        assert!(matches!(
            &snapshot.slots[..],
            [Slot::Ready { db_name }] if *db_name != original_database
        ));
        assert_eq!(snapshot.capacity.free_slots, 1);
    }

    #[tokio::test]
    async fn given_a_closed_engine_inbox_when_attaching_then_the_caller_fails_without_waiting() {
        let mut manager = start_manager(fixed_pool_config(0)).await;
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
        let manager = start_manager(fixed_pool_config(0)).await;
        let tracker = manager.tracker.clone();
        let inbox = manager.worker_inbox_tx.clone();
        let io =
            WorkerEngineIO::new(inbox.clone(), tracker.clone(), manager.shutdown_token.clone());
        let (reply, mut received) = tokio::sync::oneshot::channel();
        io.send_delayed_message(EngineMessage::Barrier { reply }, 60_000, CancellationToken::new())
            .expect("delayed message task must be scheduled");

        assert_eq!(tracker.len(), 1);
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
