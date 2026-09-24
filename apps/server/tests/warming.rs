#![cfg(unix)]

use std::{net::SocketAddr, process::Stdio, time::Duration};

use pgtest_database_operations::testcontainer::pg_container_config;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
};
use tokio_postgres::{Config, NoTls};

#[tokio::test]
async fn environment_server_warms_both_listeners_and_drains_on_interrupt() {
    tokio::time::timeout(Duration::from_secs(45), async {
        for wait in ["5000", "0"] {
            let pg = pg_container_config().await;
            let directory = tempfile::Builder::new().prefix("pgsrv-").tempdir_in("/tmp").unwrap();
            let app = format!("server-warm-{wait}");
            let params =
                serde_json::json!({"client_encoding":"UTF8", "application_name":app}).to_string();
            let mut child = Command::new(env!("CARGO_BIN_EXE_server"))
                .env("PGTEST_PG_HOST", &pg.pgtest_pg_host)
                .env("PGTEST_PG_PORT", pg.pgtest_pg_port.to_string())
                .env("PGTEST_PG_USER", &pg.pgtest_pg_user)
                .env("PGTEST_PG_DATABASE", &pg.pgtest_pg_database)
                .env("PGTEST_LISTEN_ADDR", "127.0.0.1")
                .env("PGTEST_LISTEN_PORT", "0")
                .env("PGTEST_UNIX_SOCKET_DIR", directory.path())
                .env("PGTEST_UNIX_SOCKET_PORT", "6432")
                .env("PGTEST_POOL_INITIAL_SIZE", "1")
                .env("PGTEST_POOL_GROW_BATCH_SIZE", "0")
                .env("PGTEST_CONNECTION_WARM_COUNT", "2")
                .env("PGTEST_CONNECTION_WARM_MAX_TOTAL", "2")
                .env("PGTEST_CONNECTION_WARM_CONCURRENCY", "2")
                .env("PGTEST_CONNECTION_WARM_STARTUP_WAIT_MS", wait)
                .env("PGTEST_CONNECTION_WARM_PARAMS", params)
                .env("RUST_LOG", "info")
                .env("NO_COLOR", "1")
                .kill_on_drop(true)
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
            let mut address: Option<SocketAddr> = None;
            let mut unix_ready = false;
            while address.is_none() || !unix_ready {
                let line =
                    lines.next_line().await.unwrap().expect("server stopped before readiness");
                if line.contains("pgtest server listening") {
                    address = Some(line.split("address=").nth(1).unwrap().trim().parse().unwrap());
                }
                unix_ready |= line.contains("pgtest Unix socket listening");
            }
            let logs = tokio::spawn(async move {
                let mut text = String::new();
                lines.into_inner().read_to_string(&mut text).await.unwrap();
                text
            });
            let address = address.unwrap();
            let (admin, connection) = Config::new()
                .host(&pg.pgtest_pg_host)
                .port(pg.pgtest_pg_port)
                .user("postgres")
                .dbname("postgres")
                .connect(NoTls)
                .await
                .unwrap();
            let admin_task = tokio::spawn(connection);
            let initial: Vec<i32> = admin
                .query("SELECT pid FROM pg_stat_activity WHERE application_name = $1", &[&app])
                .await
                .unwrap()
                .iter()
                .map(|row| row.get(0))
                .collect();
            if wait != "0" {
                assert_eq!(initial.len(), 2);
            }
            let mut clients = Vec::new();
            let mut pids = Vec::new();
            for unix in [false, true] {
                let mut config = Config::new();
                config
                    .user("postgres")
                    .dbname(format!("{}/both", pg.pgtest_pg_database))
                    .application_name(&app);
                if unix {
                    config.host_path(directory.path()).port(6432);
                } else {
                    config.host("127.0.0.1").port(address.port());
                }
                let (client, connection) = config.connect(NoTls).await.unwrap();
                let task = tokio::spawn(connection);
                let pid: i32 =
                    client.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0);
                if wait != "0" {
                    assert!(initial.contains(&pid));
                }
                pids.push(pid);
                clients.push((client, task));
            }
            assert_ne!(pids[0], pids[1]);
            assert!(
                Command::new("kill")
                    .args(["-INT", &child.id().unwrap().to_string()])
                    .status()
                    .await
                    .unwrap()
                    .success()
            );
            let status = child.wait().await.unwrap();
            assert!(status.success(), "{}", logs.await.unwrap());
            for (client, task) in clients {
                let _ = task.await.unwrap();
                assert!(client.is_closed());
            }
            assert!(!directory.path().join(".s.PGSQL.6432").exists());
            assert!(tokio::net::TcpStream::connect(address).await.is_err());
            while admin
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
                    &[&app],
                )
                .await
                .unwrap()
                .get::<_, i64>(0)
                != 0
            {
                tokio::task::yield_now().await;
            }
            drop(admin);
            admin_task.await.unwrap().unwrap();
        }
    })
    .await
    .unwrap();
}
