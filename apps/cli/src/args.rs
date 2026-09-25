use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use bpaf::{Bpaf, Parser, ShellComp};
use pgtest::worker_engine::core::WorkerEngineConfig;
use pgtest_database_operations::manager::config::PostgresConfig;
use pgtest_pg_wire::connection_warm::ConnectionWarmConfig;

/// Run pgtest against an existing PostgreSQL instance.
#[derive(Clone, Debug, Bpaf)]
#[bpaf(options, generate(options), version(crate::version::display().as_str()))]
pub enum Command {
    /// Start the test database server.
    #[bpaf(command)]
    Serve(#[bpaf(external(serve_options))] ServeOptions),
    /// Show the compiled version and commit SHA.
    #[bpaf(command)]
    Version,
}

#[derive(Clone, Debug, Bpaf)]
#[bpaf(generate(parse_serve_options))]
pub struct ServeOptions {
    /// Upstream hostname, IP address, or Unix socket directory.
    #[bpaf(long, argument("HOST"), guard(|value| !value.is_empty(), "--pg-host cannot be empty"))]
    pub pg_host: String,
    /// Upstream PostgreSQL port (also used for its Unix socket filename).
    #[bpaf(long, argument("PORT"), guard(positive_port, "--pg-port must be greater than zero"))]
    pub pg_port: u16,
    /// Upstream PostgreSQL user.
    #[bpaf(long, argument("USER"), guard(|value| !value.is_empty(), "--pg-user cannot be empty"))]
    pub pg_user: String,
    /// Existing PostgreSQL template database to clone for tests.
    #[bpaf(long, argument("DATABASE"), guard(|value| !value.is_empty(), "--pg-database cannot be empty"))]
    pub pg_database: String,
    /// Enable TCP on this IP address, e.g. 127.0.0.1 or ::1.
    #[bpaf(long, argument("IP"), optional)]
    pub listen_addr: Option<IpAddr>,
    /// TCP port; defaults to 6432. Zero selects an available port.
    #[bpaf(long, argument("PORT"), optional)]
    pub listen_port: Option<u16>,
    /// Existing directory for the frontend Unix socket.
    #[bpaf(long, argument("DIR"), complete_shell(ShellComp::Dir { mask: None }), optional)]
    pub unix_socket_dir: Option<PathBuf>,
    /// Unix socket filename port; defaults to 6432, independent of TCP.
    #[bpaf(
        long,
        argument("PORT"),
        guard(positive_port, "--unix-socket-port must be greater than zero"),
        optional
    )]
    pub unix_socket_port: Option<u16>,
    /// Maximum PostgreSQL connections used for database creation.
    #[bpaf(
        long,
        argument("COUNT"),
        guard(positive_pool, "--creation-pool-connection must be greater than zero"),
        fallback(10)
    )]
    pub creation_pool_connection: u32,
    /// Maximum PostgreSQL connections used for database cleanup.
    #[bpaf(
        long,
        argument("COUNT"),
        guard(positive_pool, "--cleanup-pool-connection must be greater than zero"),
        fallback(5)
    )]
    pub cleanup_pool_connection: u32,
    /// Initial number of ready test databases.
    #[bpaf(long, argument("COUNT"), fallback(16))]
    pub pool_initial_size: u16,
    /// Ready database threshold that triggers replenishment.
    #[bpaf(long, argument("COUNT"), fallback(8))]
    pub pool_starvation_threshold: u16,
    /// Number of databases per growth batch; zero disables growth.
    #[bpaf(long, argument("COUNT"), fallback(16))]
    pub pool_grow_batch_size: u16,
    /// Maximum lease lifetime in milliseconds.
    #[bpaf(long, argument("MS"), fallback(30000))]
    pub lease_claim_timeout_ms: u64,
    /// Maximum admitted lease IDs, including closed IDs.
    #[bpaf(
        long,
        argument("COUNT"),
        guard(positive_capacity, "--max-lease-records must be greater than zero"),
        fallback(100000)
    )]
    pub max_lease_records: usize,
    /// Lifetime warm handoff budget per physical database; zero disables
    /// warming.
    #[bpaf(long, argument("COUNT"), fallback(0))]
    pub connection_warm_count: u16,
    /// Global cap on idle connections plus in-flight warm attempts; must be
    /// positive.
    #[bpaf(long, argument("COUNT"), fallback(32))]
    pub connection_warm_max_total: usize,
    /// Maximum concurrent warm attempts; must be positive.
    #[bpaf(long, argument("COUNT"), fallback(4))]
    pub connection_warm_concurrency: usize,
    /// Initial warm-up wait in milliseconds; zero selects background-only
    /// warming.
    #[bpaf(long, argument("MS"), fallback(5000))]
    pub connection_warm_startup_wait_ms: u64,
    /// Warm startup parameters as a JSON string-to-string object; excludes
    /// database and replication.
    #[bpaf(long, argument("JSON"), fallback(String::from("{}")))]
    pub connection_warm_params: String,
    /// Tracing filter; defaults to info. Does not read RUST_LOG.
    #[bpaf(long, argument("FILTER"), fallback(String::from("info")))]
    pub log_filter: String,
}

fn positive_port(value: &u16) -> bool {
    *value > 0
}

fn positive_pool(value: &u32) -> bool {
    *value > 0
}

fn positive_capacity(value: &usize) -> bool {
    *value > 0
}

fn serve_options() -> impl Parser<ServeOptions> {
    parse_serve_options()
        .guard(
            |options| options.listen_addr.is_some() || options.unix_socket_dir.is_some(),
            "provide --listen-addr, --unix-socket-dir, or both",
        )
        .guard(
            |options| options.listen_port.is_none() || options.listen_addr.is_some(),
            "--listen-port requires --listen-addr",
        )
        .guard(
            |options| options.unix_socket_port.is_none() || options.unix_socket_dir.is_some(),
            "--unix-socket-port requires --unix-socket-dir",
        )
}

impl ServeOptions {
    pub fn tcp_address(&self) -> Option<SocketAddr> {
        self.listen_addr.map(|address| SocketAddr::new(address, self.listen_port.unwrap_or(6432)))
    }

    pub fn validate(&self) -> Result<()> {
        // Validate now, before database work. Pool integration will consume
        // this config.
        self.connection_warm_config()?;
        if let Some(directory) = &self.unix_socket_dir {
            ensure!(cfg!(unix), "Unix sockets are unsupported on this platform");
            ensure!(directory.is_dir(), "--unix-socket-dir must be an existing directory");
        }
        Ok(())
    }

    pub fn connection_warm_config(&self) -> Result<ConnectionWarmConfig> {
        let params = serde_json::from_str(&self.connection_warm_params)
            .context("--connection-warm-params must be a JSON object with string values")?;
        ConnectionWarmConfig::try_new(
            self.connection_warm_count,
            self.connection_warm_max_total,
            self.connection_warm_concurrency,
            Duration::from_millis(self.connection_warm_startup_wait_ms),
            params,
        )
        .context("invalid --connection-warm-* configuration")
    }

    pub fn postgres_config(&self) -> PostgresConfig {
        PostgresConfig {
            pgtest_pg_host: self.pg_host.clone(),
            pgtest_pg_port: self.pg_port,
            pgtest_pg_user: self.pg_user.clone(),
            pgtest_pg_database: self.pg_database.clone(),
            pgtest_pg_creation_pool_connection: self.creation_pool_connection,
            pgtest_pg_cleanup_pool_connection: self.cleanup_pool_connection,
        }
    }

    pub fn engine_config(&self) -> WorkerEngineConfig {
        WorkerEngineConfig {
            initial_slots: self.pool_initial_size,
            starvation_threshold: self.pool_starvation_threshold,
            grow_batch_size: self.pool_grow_batch_size,
            lease_claim_timeout_ms: self.lease_claim_timeout_ms,
            max_lease_records: self.max_lease_records,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UPSTREAM: &[&str] = &[
        "serve",
        "--pg-host",
        "localhost",
        "--pg-port",
        "5432",
        "--pg-user",
        "postgres",
        "--pg-database",
        "template",
    ];

    fn parse(extra: &[&str]) -> Result<ServeOptions, bpaf::ParseFailure> {
        let args = [UPSTREAM, extra].concat();
        options().run_inner(args.as_slice()).map(|command| match command {
            Command::Serve(value) => value,
            Command::Version => panic!("expected serve command"),
        })
    }

    #[test]
    fn listeners_are_explicit_and_can_be_combined() {
        assert!(parse(&[]).is_err());
        let tcp = parse(&["--listen-addr", "::1", "--listen-port", "0"]).unwrap();
        assert!(tcp.unix_socket_dir.is_none());
        assert_eq!(tcp.tcp_address().unwrap().port(), 0);
        let unix = parse(&["--unix-socket-dir", "/tmp", "--unix-socket-port", "7432"]).unwrap();
        assert!(unix.listen_addr.is_none());
        assert_eq!(unix.unix_socket_port, Some(7432));
        assert!(parse(&["--listen-addr", "127.0.0.1", "--unix-socket-dir", "/tmp"]).is_ok());
        assert!(parse(&["--listen-addr", "127.0.0.1", "--unix-socket-port", "7432"]).is_err());
        assert!(parse(&["--unix-socket-dir", "/tmp", "--listen-port", "7432"]).is_err());
        let tcp = parse(&["--listen-addr", "127.0.0.1", "--listen-port", "8432"]).unwrap();
        assert_eq!(tcp.tcp_address().unwrap(), "127.0.0.1:8432".parse().unwrap());
    }

    #[test]
    fn each_upstream_value_is_required() {
        for index in [1, 3, 5, 7] {
            let mut args = UPSTREAM.to_vec();
            args.drain(index..index + 2);
            args.extend(["--listen-addr", "127.0.0.1"]);
            assert!(options().run_inner(args.as_slice()).is_err());
        }
    }

    #[test]
    fn defaults_match_server_configuration() {
        let options = parse(&["--listen-addr", "127.0.0.1"]).unwrap();
        assert_eq!(options.tcp_address().unwrap().port(), 6432);
        let postgres = options.postgres_config();
        assert_eq!(postgres.pgtest_pg_host, "localhost");
        assert_eq!(postgres.pgtest_pg_port, 5432);
        assert_eq!(postgres.pgtest_pg_user, "postgres");
        assert_eq!(postgres.pgtest_pg_database, "template");
        assert_eq!(postgres.pgtest_pg_creation_pool_connection, 10);
        assert_eq!(postgres.pgtest_pg_cleanup_pool_connection, 5);
        let engine = options.engine_config();
        assert_eq!(engine.initial_slots, 16);
        assert_eq!(engine.starvation_threshold, 8);
        assert_eq!(engine.grow_batch_size, 16);
        assert_eq!(engine.lease_claim_timeout_ms, 30000);
        assert_eq!(engine.max_lease_records, 100000);
        assert_eq!(options.unix_socket_port.unwrap_or(6432), 6432);
        assert_eq!(options.log_filter, "info");
        assert_eq!(options.connection_warm_config().unwrap(), ConnectionWarmConfig::default());
    }

    #[test]
    fn warm_overrides_reach_the_validated_config() {
        let options = parse(&[
            "--listen-addr",
            "127.0.0.1",
            "--connection-warm-count",
            "8",
            "--connection-warm-max-total",
            "2",
            "--connection-warm-concurrency",
            "1",
            "--connection-warm-startup-wait-ms",
            "250",
            "--connection-warm-params",
            r#"{"user":"test_user","application_name":"vitest","options":"-c search_path=public"}"#,
        ])
        .unwrap();
        options.validate().unwrap();
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
        assert_eq!(options.connection_warm_config().unwrap(), expected);
    }

    #[test]
    fn warm_configuration_accepts_zero_target_and_startup_wait() {
        let options = parse(&[
            "--listen-addr",
            "127.0.0.1",
            "--connection-warm-count",
            "0",
            "--connection-warm-startup-wait-ms",
            "0",
        ])
        .unwrap();
        options.validate().unwrap();
        assert_eq!(
            options.connection_warm_config().unwrap(),
            ConnectionWarmConfig::try_new(0, 32, 4, Duration::ZERO, Default::default()).unwrap()
        );
    }

    #[test]
    fn invalid_warm_configuration_is_rejected_before_startup() {
        for count in ["0", "1"] {
            for (flag, value) in [
                ("--connection-warm-max-total", "0"),
                ("--connection-warm-concurrency", "0"),
                ("--connection-warm-params", r#"{"database":""}"#),
                ("--connection-warm-params", r#"{"replication":"false"}"#),
                ("--connection-warm-params", r#"{"user":42}"#),
                ("--connection-warm-params", r#"{"user":null}"#),
                ("--connection-warm-params", r#"{"options":{}}"#),
                ("--connection-warm-params", "[]"),
                ("--connection-warm-params", "null"),
                ("--connection-warm-params", "invalid"),
                ("--connection-warm-params", ""),
            ] {
                let options = parse(&[
                    "--listen-addr",
                    "127.0.0.1",
                    "--connection-warm-count",
                    count,
                    flag,
                    value,
                ])
                .unwrap();
                assert!(options.validate().is_err(), "accepted {flag}={value} with count={count}");
            }
        }
    }

    #[test]
    fn invalid_warm_numbers_fail_argument_parsing() {
        for (flag, value) in [
            ("--connection-warm-count", "65536"),
            ("--connection-warm-count", "-1"),
            ("--connection-warm-max-total", "-1"),
            ("--connection-warm-concurrency", "invalid"),
            ("--connection-warm-startup-wait-ms", "-1"),
            ("--connection-warm-startup-wait-ms", "18446744073709551616"),
        ] {
            assert!(parse(&["--listen-addr", "127.0.0.1", flag, value]).is_err(), "{flag}={value}");
        }
    }

    #[test]
    fn overrides_reach_the_engine_and_postgres_configs() {
        let options = parse(&[
            "--unix-socket-dir",
            "/tmp",
            "--creation-pool-connection",
            "2",
            "--cleanup-pool-connection",
            "3",
            "--pool-initial-size",
            "4",
            "--pool-starvation-threshold",
            "5",
            "--pool-grow-batch-size",
            "0",
            "--lease-claim-timeout-ms",
            "60000",
            "--max-lease-records",
            "7",
            "--log-filter",
            "warn",
        ])
        .unwrap();
        let postgres = options.postgres_config();
        assert_eq!(postgres.pgtest_pg_creation_pool_connection, 2);
        assert_eq!(postgres.pgtest_pg_cleanup_pool_connection, 3);
        let engine = options.engine_config();
        assert_eq!(engine.initial_slots, 4);
        assert_eq!(engine.starvation_threshold, 5);
        assert_eq!(engine.grow_batch_size, 0);
        assert_eq!(engine.lease_claim_timeout_ms, 60000);
        assert_eq!(engine.max_lease_records, 7);
        assert_eq!(options.log_filter, "warn");
    }

    #[test]
    fn invalid_values_are_rejected() {
        for (flag, value) in [
            ("--creation-pool-connection", "0"),
            ("--cleanup-pool-connection", "0"),
            ("--max-lease-records", "0"),
            ("--pool-initial-size", "65536"),
            ("--pool-grow-batch-size", "-1"),
            ("--unix-socket-port", "0"),
            ("--unix-socket-port", "65536"),
            ("--listen-addr", "localhost:6432"),
            ("--listen-addr", "127.0.0.1:65536"),
            ("--listen-addr", "[::1]:6432"),
        ] {
            assert!(parse(&["--unix-socket-dir", "/tmp", flag, value]).is_err(), "{flag}={value}");
        }
        for port in ["65536", "-1", "invalid", ""] {
            assert!(parse(&["--listen-addr", "127.0.0.1", "--listen-port", port]).is_err());
        }
        for (index, value) in [(2, ""), (4, "0"), (4, "65536"), (6, ""), (8, "")] {
            let mut args = UPSTREAM.to_vec();
            args[index] = value;
            args.extend(["--listen-addr", "127.0.0.1"]);
            assert!(options().run_inner(args.as_slice()).is_err());
        }
    }

    #[test]
    fn socket_directory_must_exist() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing");
        let options = parse(&["--unix-socket-dir", missing.to_str().unwrap()]).unwrap();
        assert!(options.validate().is_err());
        assert!(!missing.exists());
    }

    #[test]
    fn help_and_version_do_not_require_server_arguments() {
        for args in [&["--help"][..], &["--version"][..], &["serve", "--help"][..]] {
            let text = options().run_inner(args).unwrap_err().unwrap_stdout();
            assert!(!text.is_empty());
        }
    }
}
