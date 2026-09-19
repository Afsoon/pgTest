//! TCP and Unix listeners and their connection task lifecycles.

use std::{net::SocketAddr, sync::Arc};

use pgtest::worker_manager::WorkerEngineManager;
use thiserror::Error;
use tokio::task::JoinSet;

use crate::connection::{ClientStream, handle_connection};
#[cfg(unix)]
pub use crate::unix_listener::UnixWireListener;
pub use crate::{connection::parse_connection_field, postgres_upstream::RawBytes};

pub struct WireListener {}

#[derive(Error, Debug)]
pub enum WireError {
    #[error("failed to open wire listener: {0}")]
    Listen(#[from] std::io::Error),
}

const DEFAULT_WIRE_PORT: u16 = 6432;

#[hotpath::measure_all]
impl WireListener {
    #[hotpath::skip]
    pub fn new() -> Self {
        Self {}
    }

    pub async fn run(manager: Arc<WorkerEngineManager>) -> Result<SocketAddr, WireError> {
        let address = SocketAddr::from(([127, 0, 0, 1], DEFAULT_WIRE_PORT));
        let listener = tokio::net::TcpListener::bind(address).await?;
        let local_address = listener.local_addr()?;

        let mut pg_connection_sessions: JoinSet<()> = JoinSet::new();

        tokio::spawn(async move {
            tracing::debug!("to wait connection");
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _peer) = match accepted {
                            Ok(client) => client,
                            Err(error) => {
                                tracing::warn!("Unable to connect, failed due: {}", error);
                                continue;
                            }
                        };

                        pg_connection_sessions.spawn(handle_connection(ClientStream::Tcp(stream), manager.clone()));
                    }
                    Some(_finished) = pg_connection_sessions.join_next(), if !pg_connection_sessions.is_empty() => {}
                }
            }
        });

        Ok(local_address)
    }

    #[cfg(unix)]
    pub async fn run_unix(
        manager: Arc<WorkerEngineManager>,
        directory: &std::path::Path,
    ) -> Result<UnixWireListener, WireError> {
        let listener = crate::unix_listener::BoundUnixListener::bind(directory, DEFAULT_WIRE_PORT)?;
        let path = listener.path().to_owned();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut sessions = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    Some(_finished) = sessions.join_next(), if !sessions.is_empty() => {}
                    accepted = listener.accept() => {
                        match accepted {
                            Ok(stream) => {
                                sessions.spawn(handle_connection(ClientStream::Unix(stream), manager.clone()));
                            }
                            Err(error) => tracing::warn!(%error, "failed to accept Unix connection"),
                        }
                    }
                }
            }
            sessions.shutdown().await;
        });
        Ok(UnixWireListener { task: Some(task), stop: Some(stop), path })
    }
}

#[cfg(test)]
mod listener_test {
    use std::sync::Arc;

    use pgtest::{worker_engine::core::WorkerEngineConfig, worker_manager::WorkerEngineManager};
    use pgtest_database_operations::testcontainer::pg_container_config;
    use tracing_test::traced_test;

    use crate::wire_listener::WireListener;

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_application_and_control_connections_share_lease_state() {
        tokio::time::timeout(std::time::Duration::from_secs(30), unix_lease_flow(false))
            .await
            .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_upstream_supports_database_pools_and_leased_sessions() {
        tokio::time::timeout(std::time::Duration::from_secs(30), unix_lease_flow(true))
            .await
            .unwrap();
    }

    #[cfg(unix)]
    async fn unix_lease_flow(use_unix_upstream: bool) {
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let path =
            std::env::temp_dir().join(format!("pgi-{}-{use_unix_upstream}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        let directory = Directory(path);
        let mut pg_config = pg_container_config().await;
        let mut bridge_tasks = tokio::task::JoinSet::new();
        if use_unix_upstream {
            let upstream_directory = directory.0.join("upstream");
            std::fs::create_dir(&upstream_directory).unwrap();
            let listener = crate::unix_listener::BoundUnixListener::bind(
                &upstream_directory,
                pg_config.pgtest_pg_port,
            )
            .unwrap();
            let host = pg_config.pgtest_pg_host.clone();
            let port = pg_config.pgtest_pg_port;
            bridge_tasks.spawn(async move {
                let mut sessions = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let mut unix_stream = accepted.unwrap();
                            let host = host.clone();
                            sessions.spawn(async move {
                                let mut tcp_stream = tokio::net::TcpStream::connect((host.as_str(), port)).await.unwrap();
                                let _ = tokio::io::copy_bidirectional(&mut unix_stream, &mut tcp_stream).await;
                            });
                        }
                        Some(result) = sessions.join_next(), if !sessions.is_empty() => { result.unwrap(); }
                    }
                }
            });
            pg_config.pgtest_pg_host = upstream_directory.to_str().unwrap().to_owned();
        }
        let template = pg_config.pgtest_pg_database.clone();
        let engine = Arc::new(
            WorkerEngineManager::start(pg_config, WorkerEngineConfig::default()).await.unwrap(),
        );
        let listener = WireListener::run_unix(engine.clone(), &directory.0).await.unwrap();
        let socket_path = listener.path().to_owned();
        let mut config = tokio_postgres::Config::new();
        config.host_path(&directory.0).port(6432).user("postgres");
        let mut control_config = config.clone();
        control_config.dbname("pgtest");
        config.dbname(&format!("{template}/unix-test"));

        let (application, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let application_task = tokio::spawn(connection);
        let row = application.query_one("SELECT current_database(), 42::int4", &[]).await.unwrap();
        assert!(row.get::<_, &str>(0).starts_with(&format!("{template}_")));
        assert_eq!(row.get::<_, i32>(1), 42);

        let (control, connection) = control_config.connect(tokio_postgres::NoTls).await.unwrap();
        let control_task = tokio::spawn(connection);
        assert_eq!(control.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0), 1);
        drop(application);
        application_task.await.unwrap().unwrap();
        let row =
            control.query_one("SELECT pgtest_release($1::text)", &[&"unix-test"]).await.unwrap();
        assert!(row.get::<_, bool>(0));
        assert!(config.connect(tokio_postgres::NoTls).await.is_err());
        drop(control);
        control_task.await.unwrap().unwrap();

        listener.shutdown().await;
        assert!(!socket_path.exists());
        // The listener no longer retains the manager once its accept task
        // exits.
        Arc::try_unwrap(engine)
            .unwrap_or_else(|_| panic!("listener retained the manager"))
            .shutdown()
            .await;
        bridge_tasks.shutdown().await;
    }

    #[tokio::test]
    #[traced_test]
    async fn listener_test() {
        let pg_config = pg_container_config().await;
        let template_database = pg_config.pgtest_pg_database.clone();
        let worker_engine_config = WorkerEngineConfig::default();

        let engine = Arc::new(
            WorkerEngineManager::start(pg_config, worker_engine_config)
                .await
                .expect("Manager started up"),
        );

        let address = WireListener::run(engine.clone()).await.unwrap();

        let mut config = tokio_postgres::Config::new();
        config.host("127.0.0.1").port(address.port()).user("postgres");
        for (database, code) in [
            ("postgres".to_owned(), "22023"),
            (format!("{template_database}/"), "22023"),
            (format!("{template_database}/a/b"), "22023"),
            ("unknown_template/lease".to_owned(), "3D000"),
        ] {
            config.dbname(&database);
            let error = match config.connect(tokio_postgres::NoTls).await {
                Err(error) => error,
                Ok(_) => panic!("unexpectedly accepted database {database:?}"),
            };
            assert_eq!(error.as_db_error().unwrap().code().code(), code);
        }

        config.dbname("pgtest");
        let (control, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let control_task = tokio::spawn(connection);
        assert_eq!(control.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0), 1);
        drop(control);
        control_task.await.unwrap().unwrap();

        let conn_str = format!(
            "host=127.0.0.1 port={} user=postgres password=ignored-by-trust \
             dbname={template_database}/123412312",
            address.port()
        );

        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await.unwrap();
        let conn_handle = tokio::spawn(connection);

        let row = client
            .query_one("SELECT 1::int4 AS x, current_database() AS database", &[])
            .await
            .unwrap();
        assert_eq!(row.get::<_, i32>("x"), 1);
        let database: &str = row.get("database");
        assert_ne!(database, template_database);
        assert!(database.starts_with(&format!("{template_database}_")));

        drop(client);
        let _ = conn_handle.await;
    }
}
