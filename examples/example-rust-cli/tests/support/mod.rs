use std::{
    net::SocketAddr,
    panic::{AssertUnwindSafe, resume_unwind},
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use futures_util::FutureExt;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{ContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};
use tokio_postgres::{Client, Config, NoTls};

pub struct PgTest {
    pub address: SocketAddr,
    child: Child,
    logs: JoinHandle<()>,
    postgres: ContainerAsync<GenericImage>,
}

impl PgTest {
    pub async fn start() -> Result<Self> {
        let binary = build_cli().await?;
        let postgres = GenericImage::new("postgres", "18-alpine")
            .with_exposed_port(ContainerPort::Tcp(5432))
            .with_wait_for(WaitFor::log(
                LogWaitStrategy::stdout_or_stderr("database system is ready to accept connections")
                    .with_times(2),
            ))
            .with_env_var("POSTGRES_DB", "test_template")
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
            .with_copy_to(
                "/docker-entrypoint-initdb.d/schema.sql",
                include_bytes!("../schema.sql").to_vec(),
            )
            .with_cmd([
                "postgres",
                "-c",
                "file_copy_method=clone",
                "-c",
                "fsync=off",
                "-c",
                "synchronous_commit=off",
                "-c",
                "full_page_writes=off",
                "-c",
                "wal_level=minimal",
                "-c",
                "max_wal_senders=0",
                "-c",
                "archive_mode=off",
                "-c",
                "summarize_wal=off",
                "-c",
                "autovacuum=off",
                "-c",
                "random_page_cost=1.1",
            ])
            .with_startup_timeout(Duration::from_secs(90))
            .start()
            .await?;

        let mut child = Command::new(binary)
            .args([
                "serve",
                "--pg-host",
                &postgres.get_host().await?.to_string(),
                "--pg-port",
                &postgres.get_host_port_ipv4(5432).await?.to_string(),
                "--pg-user",
                "postgres",
                "--pg-database",
                "test_template",
                "--listen-addr",
                "127.0.0.1",
                "--listen-port",
                "0",
                "--lease-claim-timeout-ms",
                "120000",
            ])
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .context("start the PgTest CLI")?;

        let stderr = child.stderr.take().context("capture CLI logs")?;
        let (ready, receiver) = oneshot::channel();
        let logs = tokio::spawn(async move {
            let mut ready = Some(ready);
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                eprintln!("{line}");
                if line.contains("pgtest TCP listening") {
                    if let Some((_, address)) = line.split_once("address=") {
                        if let (Ok(address), Some(sender)) = (address.trim().parse(), ready.take())
                        {
                            let _ = sender.send(address);
                        }
                    }
                }
            }
        });
        let address = timeout(Duration::from_secs(30), receiver)
            .await
            .context("timed out waiting for PgTest")?
            .context("PgTest exited before announcing its listener")?;

        Ok(Self { address, child, logs, postgres })
    }

    pub async fn stop(mut self) -> Result<()> {
        let shutdown: Result<()> = async {
            let pid = self.child.id().context("PgTest exited unexpectedly")?;
            let signal = Command::new("kill").args(["-TERM", &pid.to_string()]).status().await?;
            ensure!(signal.success(), "could not signal PgTest");
            let status = timeout(Duration::from_secs(10), self.child.wait()).await??;
            ensure!(status.success(), "PgTest exited with {status}");
            Ok(())
        }
        .await;
        if shutdown.is_err() {
            let _ = self.child.kill().await;
        }
        self.logs.abort();
        let removed = self.postgres.rm().await;
        shutdown?;
        removed?;
        Ok(())
    }
}

async fn build_cli() -> Result<PathBuf> {
    if let Some(binary) = std::env::var_os("PGTEST_BIN") {
        return PathBuf::from(binary).canonicalize().context("resolve PGTEST_BIN");
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize()?;
    let mut command = Command::new("rustup");
    command
        .current_dir(root)
        .args([
            "run",
            "nightly",
            "cargo",
            "build",
            "--locked",
            "-p",
            "cli",
            "--bin",
            "pgtest",
            "--message-format=json",
        ])
        .kill_on_drop(true)
        .stderr(Stdio::inherit());
    let output = timeout(Duration::from_secs(600), command.output())
        .await
        .context("CLI build exceeded ten minutes")??;
    ensure!(output.status.success(), "failed to build the PgTest CLI");
    for line in String::from_utf8(output.stdout)?.lines() {
        let message: serde_json::Value = serde_json::from_str(line)?;
        if message["reason"] == "compiler-artifact" && message["target"]["name"] == "pgtest" {
            if let Some(executable) = message["executable"].as_str() {
                return Ok(PathBuf::from(executable));
            }
        }
    }
    anyhow::bail!("Cargo did not report a PgTest executable")
}

pub struct Session {
    pub client: Client,
    connection: JoinHandle<Result<(), tokio_postgres::Error>>,
}

impl Session {
    pub async fn connect(config: &Config) -> Result<Self> {
        let (client, connection) = config.connect(NoTls).await?;
        Ok(Self { client, connection: tokio::spawn(connection) })
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.connection.abort();
    }
}

pub fn config(address: SocketAddr, database: &str) -> Config {
    let mut config = Config::new();
    config
        .host(&address.ip().to_string())
        .port(address.port())
        .user("postgres")
        .dbname(database)
        .connect_timeout(Duration::from_secs(5));
    config
}

pub async fn with_lease(
    address: SocketAddr,
    run: impl AsyncFnOnce(&Client, &Config) -> Result<()>,
) -> Result<()> {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    let lease_id =
        format!("rust-{}-{}", std::process::id(), NEXT_ID.fetch_add(1, Ordering::Relaxed));
    let application_config = config(address, &format!("test_template/{lease_id}"));
    let control = Session::connect(&config(address, "pgtest")).await?;
    let result = AssertUnwindSafe(async {
        let application = Session::connect(&application_config).await?;
        run(&application.client, &application_config).await
    })
    .catch_unwind()
    .await;
    let released = control.client.query_one("SELECT pgtest_release($1::text)", &[&lease_id]).await;
    match result {
        Ok(result) => {
            result?;
            ensure!(released?.get::<_, bool>(0), "lease release failed");
            let rejected = application_config.connect(NoTls).await;
            let error = rejected.err().context("released lease accepted another connection")?;
            ensure!(error.code().map(|code| code.code()) == Some("55000"), "{error}");
            Ok(())
        }
        Err(panic) => resume_unwind(panic),
    }
}
