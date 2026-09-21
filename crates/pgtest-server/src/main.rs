use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, ensure};
use envconfig::Envconfig;
use pgtest::{worker_engine::core::WorkerEngineConfig, worker_manager::WorkerEngineManager};
use pgtest_database_operations::manager::config::PostgresConfig;
use pgtest_pg_wire::wire_listener;
use tracing_subscriber::{EnvFilter, prelude::*};

#[derive(Envconfig)]
struct ServerConfig {
    #[envconfig(from = "PGTEST_LISTEN_ADDR", default = "127.0.0.1:6432")]
    listen_addr: SocketAddr,
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

    let engine = Arc::new(
        WorkerEngineManager::start(postgres_config, worker_engine_config)
            .await
            .context("failed to start worker engine manager")?,
    );
    let address = wire_listener::run(engine.clone(), server_config.listen_addr)
        .await
        .context("failed to start wire listener")?;
    tracing::info!(%address, "pgtest server listening");

    #[cfg(unix)]
    let unix_listener = if let Some(directory) = &server_config.unix_socket_dir {
        let listener = wire_listener::run_unix(engine, directory)
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn default_listener_stays_on_loopback() {
        let config = ServerConfig::init_from_hashmap(&HashMap::new()).unwrap();
        assert_eq!(config.listen_addr, "127.0.0.1:6432".parse().unwrap());
    }

    #[test]
    fn listener_accepts_explicit_ipv4_and_ipv6_addresses() {
        for address in ["0.0.0.0:6432", "127.0.0.1:0", "[::]:6432"] {
            let vars = HashMap::from([("PGTEST_LISTEN_ADDR".to_owned(), address.to_owned())]);
            let config = ServerConfig::init_from_hashmap(&vars).unwrap();
            assert_eq!(config.listen_addr, address.parse().unwrap());
        }
    }

    #[test]
    fn invalid_listener_address_is_rejected() {
        for address in ["localhost:6432", "0.0.0.0", "127.0.0.1:65536", ""] {
            let vars = HashMap::from([("PGTEST_LISTEN_ADDR".to_owned(), address.to_owned())]);
            assert!(ServerConfig::init_from_hashmap(&vars).is_err());
        }
    }
}
