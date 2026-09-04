use std::{collections::BTreeMap, fmt::Debug, net::SocketAddr, sync::Arc};

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use pgtest::{
    worker_engine::core::LeaseId,
    worker_manager::{LeaseSession, WorkerEngineManager},
};
use pgwire::{
    api::auth::protocol_negotiation,
    messages::{
        DecodeContext, Message, PgWireBackendMessage, PgWireFrontendMessage,
        startup::{Authentication, Startup},
    },
    tokio::server::{MaybeTls, negotiate_tls},
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional_with_sizes},
    net::TcpStream,
    task::JoinSet,
};

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

impl WireListener {
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
                let (stream, peer) = match listener.accept().await {
                    Ok(client) => client,
                    Err(error) => {
                        tracing::warn!("Unable to connect, failed due: {}", error);
                        continue;
                    }
                };

                stream.set_nodelay(true).unwrap();

                let connection_ctx = ConnectionContext { engine_manager: manager.clone() };

                pg_connection_sessions.spawn(async move {
                    WireListener::handle_coonection(stream, connection_ctx).await
                });
            }
        });

        Ok(local_address)
    }

    async fn handle_coonection(stream: tokio::net::TcpStream, connection_ctx: ConnectionContext) {
        let mut framed = match negotiate_tls::<()>(stream, None).await {
            Ok(Some(frame)) => frame,
            _ => return,
        };

        let startup = match framed.next().await {
            Some(Ok(PgWireFrontendMessage::Startup(startup_frame))) => startup_frame,
            // TODO
            Some(Ok(PgWireFrontendMessage::CancelRequest(_cancel_request))) => return,
            _ => return,
        };

        let params = &startup.parameters;

        protocol_negotiation(&mut framed, &startup).await.unwrap();

        if params.contains_key("replication") {
            // Error feature not supported
            return;
        }

        framed
            .send(PgWireBackendMessage::Authentication(
                pgwire::messages::startup::Authentication::Ok,
            ))
            .await
            .unwrap();

        let parts = framed.into_parts();
        let mut client = parts.io;
        let remaining_stream = parts.read_buf;

        let Some(database) = params.get("database") else {
            tracing::error!("Connection lack of database in the uri");
            return;
        };

        tracing::debug!("connection string is {database}");
        let decoded_connection_string =
            iri_string::percent_encode::decode::decode_whatwg_bytes(database.as_bytes())
                .into_string()
                .unwrap();

        let (database_name, lease_id) = match parse_connection_field(&decoded_connection_string) {
            Ok(parse_result) => parse_result,
            Err(_) => return,
        };
        tracing::debug!("database name {database_name} and lease_id {lease_id}");

        let Ok(lease_session) = connection_ctx.engine_manager.attach(database_name, lease_id).await
        else {
            tracing::error!("Unable to lease a database to the current connection");
            return;
        };

        let upstream_session = PostgresUpstream::connect(
            &lease_session.database_name,
            params,
            connection_ctx.engine_manager.pg_client.port,
        )
        .await
        .unwrap();

        SessionRelay::run(client, upstream_session, remaining_stream, lease_session).await;
    }
}

struct SessionRelay;

const RELAY_BUF: usize = 16 * 1024;

impl SessionRelay {
    pub async fn run(
        mut upstream_client: MaybeTls,
        mut upstream_session: UpstreamSession,
        remaining_stream: BytesMut,
        lease_session: LeaseSession,
    ) -> Result<(), ()> {
        if !remaining_stream.is_empty() {
            upstream_session.stream.write_all(&remaining_stream).await.unwrap();
        }

        upstream_client.write_all(upstream_session.session_burst.bytes()).await.unwrap();

        let result = copy_bidirectional_with_sizes(
            &mut upstream_client,
            &mut upstream_session.stream,
            RELAY_BUF,
            RELAY_BUF,
        )
        .await;

        let _ = result;

        drop(upstream_session);
        drop(lease_session);

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

impl PostgresUpstream {
    pub async fn connect(
        db_name: &str,
        client_params: &BTreeMap<String, String>,
        pg_upstream_port: u16,
    ) -> Result<UpstreamSession, ()> {
        let Ok(mut stream) = TcpStream::connect(("127.0.0.1", pg_upstream_port)).await else {
            tracing::error!("unable to connect upstream postgres");
            return Err(());
        };

        stream.set_nodelay(true).unwrap();

        let mut upstream_startup = Startup::new();
        upstream_startup.parameters = PostgresUpstream::forwardable(&client_params);
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

        let Ok(session_burst) =
            PostgresUpstream::read_until_rfq_opaque(&mut stream, &mut decode_buffer).await
        else {
            tracing::error!("unable to read the opaque rfq opaque");
            return Err(());
        };

        Ok(UpstreamSession { stream, session_burst })
    }

    fn forwardable(client_params: &BTreeMap<String, String>) -> BTreeMap<String, String> {
        const OWNED: [&str; 2] = ["database", "replication"];
        client_params
            .iter()
            .filter(|(k, _)| !OWNED.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    async fn read_until_rfq_opaque(
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

pub fn parse_connection_field(decode_raw_string: &str) -> Result<(&str, LeaseId), ()> {
    let mut parts = decode_raw_string.splitn(3, '/');
    let database_name = parts.next().unwrap();
    let Some(lease_id) = parts.next() else { return Err(()) };

    Ok((database_name, LeaseId::new(lease_id.to_owned())))
}

#[cfg(test)]
mod listener_test {
    use std::sync::Arc;

    use pgtest::{
        pg_container_config, worker_engine::core::WorkerEngineConfig,
        worker_manager::WorkerEngineManager,
    };
    use tracing_test::traced_test;

    use crate::wire_listener::WireListener;

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
