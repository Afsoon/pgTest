use crate::{utils::ReadString, worker_engine::errors::PostgresDDLClientError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatabaseId(pub u64);

#[derive(Debug)]
pub struct CreateDatabase {
    pub database_id: DatabaseId,
}

#[derive(Debug)]
pub struct CleanupDatabase {
    pub database_id: DatabaseId,
    pub database_name: ReadString,
}

#[derive(Debug)]
pub enum DatabaseWorkerMessages {
    CreationFinished { database_id: DatabaseId, result: Result<ReadString, PostgresDDLClientError> },
    CleanupFinished { database_id: DatabaseId, result: Result<(), PostgresDDLClientError> },
}
