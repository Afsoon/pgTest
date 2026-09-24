use std::{future::Future, pin::Pin};

use crate::worker_engine::{database_jobs::DatabaseId, errors::PostgresDDLClientError};

pub type DatabaseDrain<'a> =
    Pin<Box<dyn Future<Output = Result<(), PostgresDDLClientError>> + Send + 'a>>;

pub trait DatabaseLifecycle: Send + Sync + 'static {
    fn database_ready(&self, database_id: DatabaseId, database_name: &str);
    fn database_retired(&self, database_id: DatabaseId);

    /// Wait for resources belonging to a retired database before physical
    /// deletion. Runs in the cleanup worker, never the engine message loop.
    /// Failure must prevent deletion. Dropping this future cancels only the
    /// wait; retirement itself must remain in effect.
    fn drain_database(&self, _database_id: DatabaseId) -> DatabaseDrain<'_> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Default)]
pub struct NoopDatabaseLifecycle;

impl DatabaseLifecycle for NoopDatabaseLifecycle {
    fn database_ready(&self, _database_id: DatabaseId, _database_name: &str) {}

    fn database_retired(&self, _database_id: DatabaseId) {}
}
