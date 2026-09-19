use std::sync::Arc;

use pgtest_database_operations::manager::{
    PostgresManager, config::PostgresConfig, errors::PostgresClientError,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use super::{
    WorkerEngineManager, WorkerEngineType,
    database_cleanup_worker::DatabaseCleanupWorker,
    database_creation_worker::DatabaseCreationWorker,
    worker_io::{DatabaseWorkerSenders, WorkerEngineIO, WorkerEngineInbox},
};
use crate::worker_engine::core::WorkerEngineConfig;

pub(super) async fn prepare_postgres(config: PostgresConfig) -> Result<Arc<PostgresManager>, ()> {
    let client = Arc::new(PostgresManager::start(config).await.map_err(log_postgres_start_error)?);

    // Keep the large, instrumented cleanup future off the startup future's
    // inline state to avoid overflowing the stack with profiling enabled.
    let _ = Box::pin(client.drop_ddl_templates_like()).await;
    Ok(client)
}

pub(super) async fn start_workers(
    postgres_client: Arc<PostgresManager>,
    worker_engine_config: WorkerEngineConfig,
) -> WorkerEngineManager {
    let (inbox_tx, inbox_rx) =
        hotpath::channel!(tokio::sync::mpsc::unbounded_channel(), label = "worker-inbox");

    let timeout_claim = worker_engine_config.lease_claim_timeout_ms;

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

    let mut worker_engine = WorkerEngineType::new(
        worker_engine_config,
        postgres_client.clone(),
        worker_engine_io,
        worker_engine_inbox,
    );

    // Preserve the stack bound when engine initialization is instrumented.
    Box::pin(worker_engine.try_init()).await;

    tracker.spawn(creation_worker.run());
    tracker.spawn(cleanup_worker.run());

    let engine_handle = tokio::spawn(async move {
        worker_engine.run().await;
        worker_engine
    });

    WorkerEngineManager {
        worker_inbox_tx: inbox_tx,
        pg_client: postgres_client,
        lease_claim_timeout: timeout_claim,
        engine_handle,
        tracker,
        shutdown_token,
    }
}

fn log_postgres_start_error(error: PostgresClientError) {
    match error {
        error @ PostgresClientError::InvalidPoolSize(_) => {
            tracing::error!(%error, "invalid PostgreSQL pool configuration");
        }
        PostgresClientError::DatabaseDoesNotExist(database_name) => {
            tracing::error!("pgTest couldn't find {database_name} database to be used as template");
        }
        PostgresClientError::UnsupportedVersion(version) => {
            tracing::error!(
                "The minimal version supported by pgTest is PostgreSQL 13; Detected PostgresSQL \
                 {version}"
            );
        }
        PostgresClientError::UnableToFetchPostgresVersion => {
            tracing::error!(
                "pgTest failed to query the Postgres version; Query used \"SELECT \
                 current_setting('server_version_num')::int8\" "
            );
        }
        PostgresClientError::UnableToFetchDatabaseList => {
            tracing::error!(
                "pgTest failed to query all databases created in Postgres; Query used \"SELECT \
                 datname from pg_database WHERE datname LIKE $1\" "
            );
        }
        PostgresClientError::UnableToConnectToPostgres(connection_string) => {
            tracing::error!(
                "Unable to connect using this connection string \"{connection_string}\" "
            );
        }
        PostgresClientError::UnexpectedServerVersionFormatFetched(server_version) => {
            tracing::error!(
                "The version query by pgTest have an unexpected format. The value obtained is \
                 {server_version}. Please report this as a bug"
            );
        }
    }
}
