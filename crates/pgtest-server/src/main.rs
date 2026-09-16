use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow, ensure};
use envconfig::Envconfig;
use pgtest::{
    postgres_manager::PostgresConfig, worker_engine::core::WorkerEngineConfig,
    worker_manager::WorkerEngineManager,
};
use pgtest_pg_wire::wire_listener::WireListener;
use tracing_subscriber::{EnvFilter, prelude::*};

#[derive(Envconfig)]
struct ServerConfig {
    #[envconfig(from = "PGTEST_UNIX_SOCKET_DIR")]
    unix_socket_dir: Option<PathBuf>,
}

#[tokio::main]
#[hotpath::main]
async fn main() -> Result<()> {
    let log_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = tracing_subscriber::registry()
        // Filter console logs separately so SQL query events reach the profiler.
        .with(tracing_subscriber::fmt::layer().with_filter(log_filter));
    // Even an identity layer would enable events rejected by the console
    // filter.
    #[cfg(feature = "hotpath")]
    let subscriber = subscriber.with(hotpath::sqlx_tracing_layer());
    subscriber.init();
    hotpath::tokio_runtime!();

    let server_config = ServerConfig::init_from_env().context("invalid server configuration")?;
    ensure!(
        cfg!(unix) || server_config.unix_socket_dir.is_none(),
        "Unix sockets are unsupported on this platform"
    );

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
    let address =
        WireListener::run(engine.clone()).await.context("failed to start wire listener")?;
    tracing::info!(%address, "pgtest server listening");

    #[cfg(unix)]
    let unix_listener = if let Some(directory) = &server_config.unix_socket_dir {
        let listener = WireListener::run_unix(engine, directory)
            .await
            .context("failed to start Unix wire listener")?;
        tracing::info!(path = %listener.path().display(), "pgtest Unix socket listening");
        Some(listener)
    } else {
        None
    };

    tokio::signal::ctrl_c().await.context("failed to wait for Ctrl-C")?;
    tracing::info!("Stopping pgtest server");
    #[cfg(unix)]
    if let Some(listener) = unix_listener {
        listener.shutdown().await;
    }
    Ok(())
}
