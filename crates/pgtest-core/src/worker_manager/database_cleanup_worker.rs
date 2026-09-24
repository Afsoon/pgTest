use std::sync::Arc;

use hotpath::wrap::tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use pgtest_database_operations::manager::PostgresManager;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    worker_engine::{
        database_jobs::{CleanupDatabase, DatabaseWorkerMessages},
        lifecycle::DatabaseLifecycle,
        messages::EngineMessage,
        traits::PostgresClient,
    },
    worker_manager::ConsumerWorker,
};

pub(super) struct DatabaseCleanupWorker<P = PostgresManager> {
    inbox_rx: UnboundedReceiver<CleanupDatabase>,
    engine_tx: UnboundedSender<EngineMessage<ConsumerWorker>>,
    tracker: TaskTracker,
    shutdown: CancellationToken,
    postgres_manager: Arc<P>,
    lifecycle: Arc<dyn DatabaseLifecycle>,
}

impl<P: PostgresClient + Send + Sync + 'static> DatabaseCleanupWorker<P> {
    pub(super) fn new(
        engine_tx: UnboundedSender<EngineMessage<ConsumerWorker>>,
        tracker: TaskTracker,
        cancel_token: CancellationToken,
        postgres_manager: Arc<P>,
        inbox_rx: UnboundedReceiver<CleanupDatabase>,
        lifecycle: Arc<dyn DatabaseLifecycle>,
    ) -> Self {
        Self { engine_tx, tracker, shutdown: cancel_token, postgres_manager, inbox_rx, lifecycle }
    }

    pub(super) async fn run(mut self) {
        loop {
            let request = tokio::select! {
                biased;

                _ = self.shutdown.cancelled() => break,

                request = self.inbox_rx.recv() => {
                    let Some(message) = request else {
                        break;
                    };
                    message
                }
            };

            let postgres_manager = self.postgres_manager.clone();
            let engine_tx = self.engine_tx.clone();
            let shutdown = self.shutdown.clone();
            let lifecycle = self.lifecycle.clone();

            self.tracker.spawn(async move {
                let CleanupDatabase { database_id, database_name } = request;

                let result = tokio::select! {
                    biased;

                    _ = shutdown.cancelled() => return,

                    result = async {
                        lifecycle.drain_database(database_id).await?;
                        postgres_manager.drop_database(&database_name).await
                    } => result,
                };

                let message =
                    EngineMessage::DatabaseWorker(DatabaseWorkerMessages::CleanupFinished {
                        database_id,
                        result,
                    });

                if engine_tx.send(message).is_err() {
                    tracing::warn!(
                        ?database_id,
                        "unable to deliver cleanup result: engine inbox closed"
                    );
                }
            });
        }
    }
}
