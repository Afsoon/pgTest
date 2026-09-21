use std::time::Duration;

use hotpath::wrap::tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use pgtest_utils::read_string::ReadString;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::worker_engine::{
    core::LeaseId,
    database_jobs::{CleanupDatabase, CreateDatabase},
    errors::IOError,
    messages::{ConsumerReply, EngineMessage},
    traits::{ConsumerIO, EngineIO, EngineInbox},
};

pub struct LeaseSession {
    pub database_name: ReadString,
    pub lease_id: LeaseId,
    generation: u64,
    pub cancellation: CancellationToken,
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

pub enum ManagerReply {
    Attached(LeaseSession),
    Engine(ConsumerReply),
}

pub struct ConsumerWorker {
    pub oneshot_channel: tokio::sync::oneshot::Sender<ManagerReply>,
    pub attachment: Option<(LeaseId, UnboundedSender<EngineMessage<ConsumerWorker>>)>,
}

impl ConsumerWorker {
    pub fn new(sender: tokio::sync::oneshot::Sender<ManagerReply>) -> Self {
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

pub struct DatabaseWorkerSenders {
    pub creation_tx: UnboundedSender<CreateDatabase>,
    pub cleanup_tx: UnboundedSender<CleanupDatabase>,
}

impl DatabaseWorkerSenders {
    pub fn init_database_worker_channels()
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

pub struct WorkerEngineIO {
    send_message: UnboundedSender<EngineMessage<ConsumerWorker>>,
    tracker: TaskTracker,
    shutdown_token: CancellationToken,
    database_worker_senders: DatabaseWorkerSenders,
}

impl WorkerEngineIO {
    pub fn new(
        send_message: UnboundedSender<EngineMessage<ConsumerWorker>>,
        tracker: TaskTracker,
        shutdown_token: CancellationToken,
        database_worker_senders: DatabaseWorkerSenders,
    ) -> Self {
        Self { send_message, tracker, shutdown_token, database_worker_senders }
    }

    // Shutdown must interrupt timers and DDL rather than wait for them.
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

impl EngineIO<ConsumerWorker> for WorkerEngineIO {
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

pub struct WorkerEngineInbox {
    receive_message: UnboundedReceiver<EngineMessage<ConsumerWorker>>,
}

impl WorkerEngineInbox {
    pub fn new(receive_message: UnboundedReceiver<EngineMessage<ConsumerWorker>>) -> Self {
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
