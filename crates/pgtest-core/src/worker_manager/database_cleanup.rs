use std::{sync::Arc, time::Duration};

use tokio::sync::Semaphore;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    utils::ReadString,
    worker_engine::{core::WorkerEngineConfig, traits::PostgresClient},
};

const INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// A bounded queue implemented with permits so there are no idle worker tasks.
/// Admission happens before spawning: both queued and running drops count
/// toward the backlog, and the task retains its permit across failures.
#[derive(Clone)]
pub(super) struct DatabaseCleanup {
    pending: Arc<Semaphore>,
    concurrency: Arc<Semaphore>,
    tracker: TaskTracker,
    shutdown: CancellationToken,
}

impl DatabaseCleanup {
    pub(super) fn new(
        config: &WorkerEngineConfig,
        tracker: TaskTracker,
        shutdown: CancellationToken,
    ) -> Self {
        assert!(config.cleanup_max_pending > 0);
        assert!(config.cleanup_concurrency > 0);
        Self {
            pending: Arc::new(Semaphore::new(usize::from(config.cleanup_max_pending))),
            concurrency: Arc::new(Semaphore::new(usize::from(config.cleanup_concurrency))),
            tracker,
            shutdown,
        }
    }

    /// Wait only for backlog capacity, not for this database's DROP to finish.
    pub(super) async fn enqueue<P: PostgresClient + Send + Sync + 'static>(
        &self,
        database_name: ReadString,
        postgres_client: Arc<P>,
    ) -> Result<(), ()> {
        let permit = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => return Err(()),
            permit = self.pending.clone().acquire_owned() => {
                permit.expect("cleanup semaphore is never closed")
            }
        };

        let cleanup = self.clone();
        self.tracker.spawn(async move {
            let _permit = permit;
            tokio::select! {
                biased;
                _ = cleanup.shutdown.cancelled() => {
                    tracing::debug!(%database_name, "database cleanup cancelled; startup sweep will retry");
                }
                _ = cleanup.drop_with_retry(&database_name, postgres_client.as_ref()) => {}
            }
        });
        Ok(())
    }

    async fn drop_with_retry<P: PostgresClient>(&self, database_name: &str, postgres_client: &P) {
        let mut retry_delay = INITIAL_RETRY_DELAY;
        loop {
            let permit =
                self.concurrency.acquire().await.expect("cleanup semaphore is never closed");
            let result = postgres_client.drop_database(database_name).await;
            drop(permit);

            match result {
                Ok(()) => return,
                Err(error) => {
                    // Never discard a failed deletion and free backlog
                    // capacity: doing so would let
                    // replacement databases grow without bound.
                    // Release the execution permit while waiting so other drops
                    // can progress, including when one database cannot be
                    // deleted.
                    tracing::error!(
                        database_name,
                        %error,
                        retry_after_secs = retry_delay.as_secs(),
                        "database cleanup failed; retaining backlog capacity until deletion succeeds"
                    );
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
                }
            }
        }
    }
}
