use std::{collections::BTreeMap, fmt::Debug, net::SocketAddr, sync::Arc, time::Duration};

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use pgtest::{
    worker_engine::{core::LeaseId, errors::AttachError},
    worker_manager::{WorkerEngineManager, worker_io::LeaseSession},
};
use pgwire::{
    api::{
        PgWireServerHandlers,
        auth::{StartupHandler, protocol_negotiation},
    },
    error::ErrorInfo,
    messages::{
        DecodeContext, Message, PgWireBackendMessage, PgWireFrontendMessage,
        startup::{Authentication, Startup},
    },
    tokio::server::{
        MaybeTls, PgWireMessageServerCodec, negotiate_tls, process_error, process_message,
    },
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional_with_sizes},
    net::TcpStream,
    task::JoinSet,
};
use tokio_util::codec::Framed;

use crate::control_panel::{PgTestControlPanel, PgTestQueryTypeControlStatement};
#[cfg(unix)]
pub use crate::unix_listener::UnixWireListener;

type ClientConnection = Framed<MaybeTls, PgWireMessageServerCodec<PgTestQueryTypeControlStatement>>;

enum ClientStream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

impl ClientStream {
    async fn startup(self) -> Option<(ClientConnection, Startup)> {
        let mut framed = match self {
            Self::Tcp(stream) => {
                negotiate_tls::<PgTestQueryTypeControlStatement>(stream, None).await.ok()??
            }
            #[cfg(unix)]
            Self::Unix(stream) => {
                use pgwire::api::{ClientInfo, DefaultClient, PgWireConnectionState};
                // pgwire uses this placeholder address for local Unix peers.
                let client = DefaultClient::new(SocketAddr::from(([127, 0, 0, 1], 0)), false);
                let mut framed =
                    Framed::new(MaybeTls::Unix(stream), PgWireMessageServerCodec::new(client));
                framed.set_state(PgWireConnectionState::AwaitingStartup);
                framed
            }
        };
        match framed.next().await {
            Some(Ok(PgWireFrontendMessage::Startup(startup))) => Some((framed, startup)),
            _ => None,
        }
    }
}

pub struct WireListener {}

#[derive(Error, Debug)]
pub enum WireError {
    #[error("failed to open wire listener: {0}")]
    Listen(#[from] std::io::Error),
}

const DEFAULT_WIRE_PORT: u16 = 6432;

struct ConnectionContext {
    engine_manager: Arc<WorkerEngineManager>,
}

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

                        let connection_ctx = ConnectionContext { engine_manager: manager.clone() };

                        pg_connection_sessions.spawn(async move {
                            WireListener::handle_connection(ClientStream::Tcp(stream), connection_ctx).await
                        });
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
                                let context = ConnectionContext { engine_manager: manager.clone() };
                                sessions.spawn(Self::handle_connection(ClientStream::Unix(stream), context));
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

    async fn handle_connection(stream: ClientStream, connection_ctx: ConnectionContext) {
        let Ok(Some((mut framed, startup))) =
            tokio::time::timeout(Duration::from_secs(60), stream.startup()).await
        else {
            return;
        };

        let params = &startup.parameters;

        if params.contains_key("replication") {
            Self::reject_connection(
                &mut framed,
                "0A000",
                "replication connections are not supported",
            )
            .await;
            return;
        }

        let Some(database) = params.get("database") else {
            Self::reject_connection(
                &mut framed,
                "3D000",
                "database is required in the connection URI",
            )
            .await;
            return;
        };

        if database == "pgtest" {
            if let Err(error) =
                Self::serve(framed, startup, connection_ctx.engine_manager.clone()).await
            {
                tracing::warn!(%error, "Control connection failed");
            }
            return;
        }

        protocol_negotiation(&mut framed, &startup).await.unwrap();

        framed
            .send(PgWireBackendMessage::Authentication(
                pgwire::messages::startup::Authentication::Ok,
            ))
            .await
            .unwrap();

        tracing::debug!("connection string is {database}");

        let (database_name, lease_id) = match parse_connection_field(&database) {
            Ok(parse_result) => parse_result,
            Err(_) => {
                Self::reject_connection(
                    &mut framed,
                    "22023",
                    "expected template/lease-id with a valid lease ID",
                )
                .await;
                return;
            }
        };
        tracing::debug!("database name {database_name} and lease_id {lease_id}");

        let lease_session =
            match connection_ctx.engine_manager.attach(database_name, lease_id).await {
                Ok(session) => session,
                Err(error) => {
                    let code = match error {
                        AttachError::InvalidLeaseId => "22023",
                        AttachError::LeaseRecordLimitReached => "53400",
                        AttachError::LeaseClosed => "55000",
                        AttachError::TemplateMismatch => "3D000",
                        _ => "08006",
                    };
                    Self::reject_connection(&mut framed, code, &error.to_string()).await;
                    return;
                }
            };
        let cancellation = lease_session.cancellation_token();
        let upstream_session = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                Self::reject_connection(&mut framed, "55000", "lease was closed during connection startup").await;
                return;
            }
            result = PostgresUpstream::connect(
                &lease_session.database_name,
                params,
                connection_ctx.engine_manager.pg_client.port,
            ) => match result {
                Ok(session) => session,
                Err(()) => {
                    Self::reject_connection(&mut framed, "08006", "unable to connect to PostgreSQL").await;
                    return;
                }
            }
        };
        let parts = framed.into_parts();
        let _ = SessionRelay::run(parts.io, upstream_session, parts.read_buf, lease_session).await;
    }

    async fn reject_connection(
        framed: &mut Framed<MaybeTls, PgWireMessageServerCodec<PgTestQueryTypeControlStatement>>,
        code: &str,
        message: &str,
    ) {
        let error = ErrorInfo::new("FATAL".into(), code.into(), message.into());
        let _ = framed.send(PgWireBackendMessage::ErrorResponse(error.into())).await;
    }

    async fn serve(
        mut framed: Framed<MaybeTls, PgWireMessageServerCodec<PgTestQueryTypeControlStatement>>,
        startup: Startup,
        manager: Arc<WorkerEngineManager>,
    ) -> std::io::Result<()> {
        let handlers = Arc::new(PgTestControlPanel::new(manager));
        let startup_handler = handlers.startup_handler();

        if let Err(error) =
            startup_handler.on_startup(&mut framed, PgWireFrontendMessage::Startup(startup)).await
        {
            process_error(&mut framed, error, false).await?;
            return Ok(());
        }

        let copy_handler = handlers.copy_handler();
        let cancel_handler = handlers.cancel_handler();

        while let Some(message) = framed.next().await {
            let message = message?;
            if matches!(message, PgWireFrontendMessage::Terminate(_)) {
                break;
            }

            let wait_for_sync = message.is_extended_query();
            // Use the concrete query handler so its Statement type matches the
            // codec.
            if let Err(error) = process_message(
                message,
                &mut framed,
                startup_handler.clone(),
                handlers.clone(),
                handlers.clone(),
                copy_handler.clone(),
                cancel_handler.clone(),
            )
            .await
            {
                process_error(&mut framed, error, wait_for_sync).await?;
            }
        }

        Ok(())
    }
}

struct SessionRelay;

const RELAY_BUF: usize = 16 * 1024;

#[hotpath::measure_all]
impl SessionRelay {
    pub async fn run(
        upstream_client: MaybeTls,
        upstream_session: UpstreamSession,
        remaining_stream: BytesMut,
        lease_session: LeaseSession,
    ) -> Result<(), ()> {
        let mut upstream_client = hotpath::io!(upstream_client, label = "client-relay");
        let mut upstream_stream = hotpath::io!(upstream_session.stream, label = "postgres-relay");
        let cancellation = lease_session.cancellation_token();
        let relay = async {
            if !remaining_stream.is_empty() {
                upstream_stream.write_all(&remaining_stream).await?;
            }
            upstream_client.write_all(upstream_session.session_burst.bytes()).await?;
            copy_bidirectional_with_sizes(
                &mut upstream_client,
                &mut upstream_stream,
                RELAY_BUF,
                RELAY_BUF,
            )
            .await?;
            Ok::<(), std::io::Error>(())
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {}
            result = relay => {
                if let Err(error) = result {
                    tracing::debug!(%error, "session relay ended with an I/O error");
                }
            }
        }
        // Both sockets and the session guard are dropped on every exit path.

        Ok(())
    }
}

struct PostgresUpstream;

pub struct RawBytes(BytesMut);
impl RawBytes {
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
impl From<BytesMut> for RawBytes {
    fn from(b: BytesMut) -> Self {
        RawBytes(b)
    }
}

struct UpstreamSession {
    pub stream: TcpStream,
    pub session_burst: RawBytes,
}

#[hotpath::measure_all]
impl PostgresUpstream {
    pub async fn connect(
        db_name: &str,
        client_params: &BTreeMap<String, String>,
        pg_upstream_port: u16,
    ) -> Result<UpstreamSession, ()> {
        let mut stream = Self::connect_tcp(pg_upstream_port).await?;
        let mut decode_buffer = Self::authenticate(&mut stream, db_name, client_params).await?;

        let Ok(session_burst) =
            Self::wait_for_ready_for_query(&mut stream, &mut decode_buffer).await
        else {
            tracing::error!("unable to read the opaque rfq opaque");
            return Err(());
        };

        Ok(UpstreamSession { stream, session_burst })
    }

    async fn connect_tcp(pg_upstream_port: u16) -> Result<TcpStream, ()> {
        let Ok(stream) = TcpStream::connect(("127.0.0.1", pg_upstream_port)).await else {
            tracing::error!("unable to connect upstream postgres");
            return Err(());
        };

        stream.set_nodelay(true).unwrap();
        Ok(stream)
    }

    // Includes sending Startup and waiting for AuthenticationOk. Preserve any
    // following bytes so the next stage can consume an already-buffered reply.
    async fn authenticate(
        stream: &mut TcpStream,
        db_name: &str,
        client_params: &BTreeMap<String, String>,
    ) -> Result<BytesMut, ()> {
        let mut upstream_startup = Startup::new();
        upstream_startup.parameters = Self::forwardable(client_params);
        upstream_startup.parameters.insert("database".into(), db_name.to_owned());

        let mut out = BytesMut::with_capacity(256);
        upstream_startup.encode(&mut out);
        let Ok(_) = stream.write_all(&out).await else {
            tracing::error!("unable to write the output buffer");
            return Err(());
        };

        let decode_context = DecodeContext::default();
        let mut decode_buffer = BytesMut::with_capacity(1024);

        loop {
            while decode_buffer.len() < 5 {
                let Ok(stream_buffer_red) = stream.read_buf(&mut decode_buffer).await else {
                    tracing::error!("failed to read the upstream output");
                    return Err(());
                };

                if stream_buffer_red == 0 {
                    tracing::error!("upstream unreachable");
                    return Err(());
                }
            }
            match decode_buffer[0] {
                b'R' => {
                    match Authentication::decode(&mut decode_buffer, &decode_context).unwrap() {
                        Some(Authentication::Ok) => break,
                        Some(_challenge) => {
                            tracing::error!(
                                "pgtest doesn't handle connection challenge, use non secure \
                                 connection"
                            );
                            // TBA if the volume has been persisted, need to
                            // recreate the volume o manually change it
                            return Err(());
                        }
                        None => {
                            tracing::debug!("Partial message. reading more");
                        }
                    }
                }
                b'E' => {
                    tracing::error!("error from parsing, TBA parsed to resend it again");
                    return Err(());
                }
                _ => {
                    tracing::error!("unexpected starting byte reading the decoding buffer");
                    return Err(());
                }
            }

            let Ok(stream_buffer_red) = stream.read_buf(&mut decode_buffer).await else {
                tracing::error!("failed to read the upstream output after partial message");
                return Err(());
            };

            if stream_buffer_red == 0 {
                tracing::error!("upstream unreachable after partial message");
                return Err(());
            }
        }

        Ok(decode_buffer)
    }

    #[hotpath::skip]
    fn forwardable(client_params: &BTreeMap<String, String>) -> BTreeMap<String, String> {
        const OWNED: [&str; 2] = ["database", "replication"];
        client_params
            .iter()
            .filter(|(k, _)| !OWNED.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    async fn wait_for_ready_for_query(
        stream: &mut TcpStream,
        buf: &mut BytesMut,
    ) -> Result<RawBytes, ()> {
        const HEADER: usize = 5;
        let mut cursor = 0usize;

        loop {
            while buf.len() < cursor + HEADER {
                let Ok(stream_buffer_red) = stream.read_buf(buf).await else {
                    tracing::error!("failed to read after the header + cursor");
                    return Err(());
                };

                if stream_buffer_red == 0 {
                    tracing::error!("upstream unreachable after the header + cursor");
                    return Err(());
                }
            }

            let tag = buf[cursor];
            let len = i32::from_be_bytes(buf[cursor + 1..cursor + 5].try_into().unwrap()) as usize;
            let frame_end = cursor + 1 + len;

            while buf.len() < frame_end {
                let Ok(stream_buffer_red) = stream.read_buf(buf).await else {
                    tracing::error!("failed to read the upstream output before the frame end");
                    return Err(());
                };

                if stream_buffer_red == 0 {
                    tracing::error!("upstream unreachable before the frame end");
                    return Err(());
                }
            }

            cursor = frame_end;

            match tag {
                b'Z' => return Ok(RawBytes::from(buf.split_to(cursor))),
                b'E' => return Ok(RawBytes::from(buf.split_to(cursor))),
                _ => {
                    tracing::debug!("tag distinct to Z or E, keeping scanning the buffer");
                    continue;
                }
            };
        }
    }
}

#[hotpath::measure]
pub fn parse_connection_field(decode_raw_string: &str) -> Result<(&str, LeaseId), ()> {
    let mut parts = decode_raw_string.splitn(3, '/');
    let database_name = parts.next().unwrap();
    let Some(lease_id) = parts.next() else { return Err(()) };

    if database_name.is_empty() || parts.next().is_some() {
        return Err(());
    }
    Ok((database_name, LeaseId::new(lease_id).map_err(|_| ())?))
}

#[cfg(test)]
mod listener_test {
    use std::sync::Arc;

    use pgtest::{worker_engine::core::WorkerEngineConfig, worker_manager::WorkerEngineManager};
    use pgtest_database_operations::testcontainer::pg_container_config;
    use tracing_test::traced_test;

    use crate::wire_listener::WireListener;

    #[test]
    fn connection_field_validates_lease_ids() {
        for input in ["", "template", "/lease", "template/", "template/a/b", "template/a\0b"] {
            assert!(super::parse_connection_field(input).is_err(), "{input:?}");
        }
        assert!(super::parse_connection_field(&format!("template/{}", "a".repeat(257))).is_err());
        for value in [" Mixed Case 雪 ".to_owned(), "é".repeat(128)] {
            let input = format!("template/{value}");
            let (database, lease) = super::parse_connection_field(&input).unwrap();
            assert_eq!(database, "template");
            assert_eq!(lease.as_ref(), value);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_application_and_control_connections_share_lease_state() {
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let path = std::env::temp_dir().join(format!("pgi-{}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        let directory = Directory(path);
        let pg_config = pg_container_config().await;
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
