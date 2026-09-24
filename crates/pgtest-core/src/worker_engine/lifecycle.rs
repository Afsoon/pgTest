use crate::worker_engine::database_jobs::DatabaseId;

pub trait DatabaseLifecycle: Send + Sync + 'static {
    fn database_ready(&self, database_id: DatabaseId, database_name: &str);
    fn database_retired(&self, database_id: DatabaseId);
}

#[derive(Default)]
pub struct NoopDatabaseLifecycle;

impl DatabaseLifecycle for NoopDatabaseLifecycle {
    fn database_ready(&self, database_id: DatabaseId, database_name: &str) {}

    fn database_retired(&self, database_id: DatabaseId) {}
}
