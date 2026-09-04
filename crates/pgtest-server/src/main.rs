use std::sync::Arc;

use anyhow::{Context, Result, anyhow, ensure};
use envconfig::Envconfig;
use pgtest::{
    postgres_manager::PostgresConfig, worker_engine::core::WorkerEngineConfig,
    worker_manager::WorkerEngineManager,
};
use pgtest_pg_wire::wire_listener::WireListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let postgres_config =
        PostgresConfig::init_from_env().context("invalid PostgreSQL configuration")?;
    let worker_engine_config =
        WorkerEngineConfig::init_from_env().context("invalid worker engine configuration")?;
    ensure!(
        worker_engine_config.initial_slots <= worker_engine_config.maximum_slots,
        "initial pool size must not exceed maximum pool size"
    );

    let engine =
        Arc::new(WorkerEngineManager::start(postgres_config, worker_engine_config).await.map_err(
            |()| anyhow!("failed to start worker engine manager; see logs for details"),
        )?);
    let address = WireListener::run(engine).await.context("failed to start wire listener")?;
    tracing::info!(%address, "pgtest server listening");

    tokio::signal::ctrl_c().await.context("failed to wait for Ctrl-C")?;
    tracing::info!("Stopping pgtest server");
    Ok(())
}
