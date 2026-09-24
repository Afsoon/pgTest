use pgtest::worker_engine::{
    errors::PostgresDDLClientError,
    lifecycle::{DatabaseDrain, DatabaseLifecycle},
};

use super::*;

impl ConnectionWarmPool {
    pub(crate) async fn drain_database(
        &self,
        database_id: DatabaseId,
    ) -> Result<(), PostgresDDLClientError> {
        // Idempotently enforce retirement even if called without the earlier
        // notification. Retired entries remain as identity tombstones.
        self.retire_database(database_id);
        let tracker = {
            let state = self.state.lock().map_err(|_| {
                PostgresDDLClientError::DatabaseDrainFailed("warm pool state lock poisoned".into())
            })?;
            let Some(entry) = state.databases.get(&database_id) else {
                return Ok(());
            };
            entry.drain.clone()
        };
        // TaskTracker supports multiple waiters without competing with the
        // scheduler's Notify. Tokens survive removal from inventory until
        // socket disposal or client handoff and complete attempt teardown.
        tracker.wait().await;
        Ok(())
    }
}

impl DatabaseLifecycle for ConnectionWarmPool {
    fn database_ready(&self, database_id: DatabaseId, database_name: &str) {
        self.register_database(database_id, database_name.to_owned());
    }

    fn database_retired(&self, database_id: DatabaseId) {
        self.retire_database(database_id);
    }

    fn drain_database(&self, database_id: DatabaseId) -> DatabaseDrain<'_> {
        Box::pin(ConnectionWarmPool::drain_database(self, database_id))
    }
}
