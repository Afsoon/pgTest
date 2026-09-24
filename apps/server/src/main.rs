#[cfg(not(feature = "hotpath-alloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::{
    net::{IpAddr, SocketAddr},
    num::NonZeroU16,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use envconfig::Envconfig;
use pgtest::{worker_engine::core::WorkerEngineConfig, worker_manager::WorkerEngineManager};
use pgtest_database_operations::manager::config::PostgresConfig;
use pgtest_pg_wire::{connection_warm::ConnectionWarmConfig, wire_listener};
use tracing_subscriber::{EnvFilter, prelude::*};

#[derive(Envconfig)]
struct ServerConfig {
    #[envconfig(from = "PGTEST_LISTEN_ADDR", default = "127.0.0.1")]
    listen_addr: IpAddr,
    #[envconfig(from = "PGTEST_LISTEN_PORT", default = "6432")]
    listen_port: u16,
    #[envconfig(from = "PGTEST_UNIX_SOCKET_DIR")]
    unix_socket_dir: Option<PathBuf>,
    #[envconfig(from = "PGTEST_UNIX_SOCKET_PORT", default = "6432")]
    unix_socket_port: NonZeroU16,
    #[envconfig(from = "PGTEST_CONNECTION_WARM_COUNT", default = "0")]
    connection_warm_count: u16,
    #[envconfig(from = "PGTEST_CONNECTION_WARM_MAX_TOTAL", default = "32")]
    connection_warm_max_total: usize,
    #[envconfig(from = "PGTEST_CONNECTION_WARM_CONCURRENCY", default = "4")]
    connection_warm_concurrency: usize,
    #[envconfig(from = "PGTEST_CONNECTION_WARM_STARTUP_WAIT_MS", default = "5000")]
    connection_warm_startup_wait_ms: u64,
    #[envconfig(from = "PGTEST_CONNECTION_WARM_PARAMS", default = "{}")]
    connection_warm_params: String,
}

impl ServerConfig {
    fn connection_warm_config(&self) -> Result<ConnectionWarmConfig> {
        let params = serde_json::from_str(&self.connection_warm_params)
            .context("PGTEST_CONNECTION_WARM_PARAMS must be a JSON object with string values")?;
        ConnectionWarmConfig::try_new(
            self.connection_warm_count,
            self.connection_warm_max_total,
            self.connection_warm_concurrency,
            Duration::from_millis(self.connection_warm_startup_wait_ms),
            params,
        )
        .context("invalid PGTEST_CONNECTION_WARM_* configuration")
    }
}

#[tokio::main]
#[hotpath::main(allocator = mimalloc::MiMalloc)]
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
    // Validate before database work; pool integration will consume this config.
    let _connection_warm_config = server_config.connection_warm_config()?;
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
    let address = wire_listener::run(
        engine.clone(),
        SocketAddr::new(server_config.listen_addr, server_config.listen_port),
    )
    .await
    .context("failed to start wire listener")?;
    tracing::info!(%address, "pgtest server listening");

    #[cfg(unix)]
    let unix_listener = if let Some(directory) = &server_config.unix_socket_dir {
        let listener = wire_listener::run_unix_on_port(
            engine,
            directory,
            server_config.unix_socket_port.get(),
        )
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
        assert_eq!(config.listen_addr, "127.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(config.listen_port, 6432);
        assert_eq!(config.unix_socket_port.get(), 6432);
        assert_eq!(config.connection_warm_config().unwrap(), ConnectionWarmConfig::default());
    }

    #[test]
    fn warm_environment_overrides_reach_the_validated_config() {
        let vars = HashMap::from([
            ("PGTEST_CONNECTION_WARM_COUNT".into(), "8".into()),
            ("PGTEST_CONNECTION_WARM_MAX_TOTAL".into(), "2".into()),
            ("PGTEST_CONNECTION_WARM_CONCURRENCY".into(), "1".into()),
            ("PGTEST_CONNECTION_WARM_STARTUP_WAIT_MS".into(), "250".into()),
            ("PGTEST_CONNECTION_WARM_PARAMS".into(),
                r#"{"user":"test_user","application_name":"vitest","options":"-c search_path=public"}"#.into()),
        ]);
        let config = ServerConfig::init_from_hashmap(&vars).unwrap();
        let expected = ConnectionWarmConfig::try_new(
            8,
            2,
            1,
            Duration::from_millis(250),
            std::collections::BTreeMap::from([
                ("user".into(), "test_user".into()),
                ("application_name".into(), "vitest".into()),
                ("options".into(), "-c search_path=public".into()),
            ]),
        )
        .unwrap();
        assert_eq!(config.connection_warm_config().unwrap(), expected);
    }

    #[test]
    fn warm_environment_accepts_zero_target_and_startup_wait() {
        let vars = HashMap::from([
            ("PGTEST_CONNECTION_WARM_COUNT".into(), "0".into()),
            ("PGTEST_CONNECTION_WARM_STARTUP_WAIT_MS".into(), "0".into()),
        ]);
        let config = ServerConfig::init_from_hashmap(&vars).unwrap();
        assert_eq!(
            config.connection_warm_config().unwrap(),
            ConnectionWarmConfig::try_new(0, 32, 4, Duration::ZERO, Default::default()).unwrap()
        );
    }

    #[test]
    fn invalid_warm_environment_is_rejected_before_startup() {
        for count in ["0", "1"] {
            for (key, value) in [
                ("PGTEST_CONNECTION_WARM_MAX_TOTAL", "0"),
                ("PGTEST_CONNECTION_WARM_CONCURRENCY", "0"),
                ("PGTEST_CONNECTION_WARM_PARAMS", r#"{"database":""}"#),
                ("PGTEST_CONNECTION_WARM_PARAMS", r#"{"replication":"false"}"#),
                ("PGTEST_CONNECTION_WARM_PARAMS", r#"{"user":42}"#),
                ("PGTEST_CONNECTION_WARM_PARAMS", r#"{"user":null}"#),
                ("PGTEST_CONNECTION_WARM_PARAMS", r#"{"options":{}}"#),
                ("PGTEST_CONNECTION_WARM_PARAMS", "[]"),
                ("PGTEST_CONNECTION_WARM_PARAMS", "null"),
                ("PGTEST_CONNECTION_WARM_PARAMS", "invalid"),
                ("PGTEST_CONNECTION_WARM_PARAMS", ""),
            ] {
                let vars = HashMap::from([
                    ("PGTEST_CONNECTION_WARM_COUNT".into(), count.into()),
                    (key.into(), value.into()),
                ]);
                let config = ServerConfig::init_from_hashmap(&vars).unwrap();
                assert!(
                    config.connection_warm_config().is_err(),
                    "accepted {key}={value} with count={count}"
                );
            }
        }
    }

    #[test]
    fn invalid_warm_numbers_fail_environment_parsing() {
        for (key, value) in [
            ("PGTEST_CONNECTION_WARM_COUNT", "65536"),
            ("PGTEST_CONNECTION_WARM_COUNT", "-1"),
            ("PGTEST_CONNECTION_WARM_MAX_TOTAL", "-1"),
            ("PGTEST_CONNECTION_WARM_CONCURRENCY", "invalid"),
            ("PGTEST_CONNECTION_WARM_STARTUP_WAIT_MS", "-1"),
            ("PGTEST_CONNECTION_WARM_STARTUP_WAIT_MS", "18446744073709551616"),
        ] {
            let vars = HashMap::from([(key.into(), value.into())]);
            assert!(ServerConfig::init_from_hashmap(&vars).is_err(), "{key}={value}");
        }
    }

    #[test]
    fn listener_accepts_explicit_ipv4_and_ipv6_addresses() {
        for address in ["0.0.0.0", "127.0.0.1", "::"] {
            let vars = HashMap::from([
                ("PGTEST_LISTEN_ADDR".to_owned(), address.to_owned()),
                ("PGTEST_LISTEN_PORT".to_owned(), "0".to_owned()),
            ]);
            let config = ServerConfig::init_from_hashmap(&vars).unwrap();
            assert_eq!(config.listen_addr, address.parse::<IpAddr>().unwrap());
            assert_eq!(config.listen_port, 0);
        }
    }

    #[test]
    fn invalid_listener_address_is_rejected() {
        for address in ["localhost", "127.0.0.1:6432", "[::]:6432", "[::]", ""] {
            let vars = HashMap::from([("PGTEST_LISTEN_ADDR".to_owned(), address.to_owned())]);
            assert!(ServerConfig::init_from_hashmap(&vars).is_err());
        }
    }

    #[test]
    fn unix_socket_port_is_independent_of_tcp_port() {
        let vars = HashMap::from([
            ("PGTEST_LISTEN_ADDR".to_owned(), "127.0.0.1".to_owned()),
            ("PGTEST_LISTEN_PORT".to_owned(), "8432".to_owned()),
            ("PGTEST_UNIX_SOCKET_DIR".to_owned(), "/tmp/pgtest".to_owned()),
            ("PGTEST_UNIX_SOCKET_PORT".to_owned(), "7432".to_owned()),
        ]);
        let config = ServerConfig::init_from_hashmap(&vars).unwrap();
        assert_eq!(config.listen_port, 8432);
        assert_eq!(config.unix_socket_port.get(), 7432);
        assert_eq!(config.unix_socket_dir, Some(PathBuf::from("/tmp/pgtest")));
    }

    #[test]
    fn invalid_tcp_ports_are_rejected() {
        for port in ["65536", "-1", "invalid", ""] {
            let vars = HashMap::from([("PGTEST_LISTEN_PORT".to_owned(), port.to_owned())]);
            assert!(ServerConfig::init_from_hashmap(&vars).is_err(), "accepted port {port:?}");
        }
    }

    #[test]
    fn invalid_unix_socket_ports_are_rejected() {
        for port in ["0", "65536", "-1", "invalid", ""] {
            let vars = HashMap::from([("PGTEST_UNIX_SOCKET_PORT".to_owned(), port.to_owned())]);
            assert!(ServerConfig::init_from_hashmap(&vars).is_err(), "accepted port {port:?}");
        }
    }
}
