use std::{net::SocketAddr, sync::Arc, time::Duration};

use futures::{SinkExt, StreamExt};
use pgtest::{
    worker_engine::{
        core::LeaseId,
        errors::{AttachError, InvalidLeaseId},
    },
    worker_manager::{WorkerEngineManager, worker_io::LeaseSession},
};
use pgwire::{
    api::auth::protocol_negotiation,
    error::ErrorInfo,
    messages::{PgWireBackendMessage, PgWireFrontendMessage, startup::Startup},
    tokio::server::{MaybeTls, PgWireMessageServerCodec, negotiate_tls},
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::{
    connection_warm::ConnectionWarmPool,
    control_panel::{self, PgTestQueryTypeControlStatement},
    postgres_upstream, session_relay,
};

pub(crate) type ClientConnection =
    Framed<MaybeTls, PgWireMessageServerCodec<PgTestQueryTypeControlStatement>>;

pub(crate) enum ClientStream {
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
                // Pgwire needs an IP, even when the connection is made through
                // unix sockets.
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

#[hotpath::measure]
pub(crate) async fn handle_connection(
    stream: ClientStream,
    manager: Arc<WorkerEngineManager>,
    warm_pool: Option<Arc<ConnectionWarmPool>>,
) {
    let Ok(Some((mut framed, startup))) =
        tokio::time::timeout(Duration::from_secs(60), stream.startup()).await
    else {
        return;
    };

    let params = &startup.parameters;

    if params.contains_key("replication") {
        reject_connection(&mut framed, "0A000", "replication connections are not supported").await;
        return;
    }

    let Some(database) = params.get("database") else {
        reject_connection(&mut framed, "3D000", "database is required in the connection URI").await;
        return;
    };

    if database == "pgtest" {
        if let Err(error) = control_panel::serve(framed, startup, manager.clone()).await {
            tracing::warn!(%error, "Control connection failed");
        }
        return;
    }

    // End startup timing after writing ReadyForQuery, before the relay starts
    // waiting for client queries. Control sessions are excluded above.
    let Some((upstream_session, lease_session)) = hotpath::measure_block!(
        "connection::client_startup_to_ready",
        start_session(&mut framed, &startup, &manager, warm_pool.as_deref()).await
    ) else {
        return;
    };
    if let Err(error) =
        session_relay::run(framed.into_inner(), upstream_session, lease_session).await
    {
        tracing::debug!(%error, "session relay ended with an I/O error");
    }
}

async fn start_session(
    framed: &mut ClientConnection,
    startup: &Startup,
    manager: &WorkerEngineManager,
    warm_pool: Option<&ConnectionWarmPool>,
) -> Option<(postgres_upstream::UpstreamSession, LeaseSession)> {
    let params = &startup.parameters;
    let database = params.get("database")?;
    if let Err(error) = protocol_negotiation(framed, startup).await {
        tracing::debug!(%error, "client protocol negotiation failed");
        return None;
    }

    if let Err(error) = framed
        .send(PgWireBackendMessage::Authentication(pgwire::messages::startup::Authentication::Ok))
        .await
    {
        tracing::debug!(%error, "unable to send client authentication response");
        return None;
    }

    tracing::debug!("connection string is {database}");

    let (database_name, lease_id) = match parse_connection_field(&database) {
        Ok(parse_result) => parse_result,
        Err(_) => {
            reject_connection(framed, "22023", "expected template/lease-id with a valid lease ID")
                .await;
            return None;
        }
    };
    tracing::debug!("database name {database_name} and lease_id {lease_id}");

    let lease_session = match manager.attach(database_name, lease_id).await {
        Ok(session) => session,
        Err(error) => {
            let code = match error {
                AttachError::InvalidLeaseId => "22023",
                AttachError::LeaseRecordLimitReached => "53400",
                AttachError::LeaseClosed => "55000",
                AttachError::TemplateMismatch => "3D000",
                _ => "08006",
            };
            reject_connection(framed, code, &error.to_string()).await;
            return None;
        }
    };
    let cancellation = lease_session.cancellation_token();
    let warm_session = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            reject_connection(framed, "55000", "lease was closed during connection startup").await;
            return None;
        }
        session = async {
            // Checkout belongs inside the cancellation race: a closed lease
            // must not consume a spare. A miss never waits for replenishment.
            hotpath::measure_block!("connection::warm_checkout", {
                warm_pool.and_then(|pool| pool.try_checkout(lease_session.database_id, params))
            })
        } => session,
    };
    let is_warm = warm_session.is_some();
    let startup = async {
        let mut session = match warm_session {
            Some(session) => session,
            None => {
                let result = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        reject_connection(framed, "55000", "lease was closed during connection startup").await;
                        return None;
                    }
                    result = async { hotpath::measure_block!(
                        "connection::cold_upstream_startup",
                        postgres_upstream::connect(
                            &lease_session.database_name,
                            params,
                            &manager.pg_client.host,
                            manager.pg_client.port,
                        ).await
                    ) } => result,
                };
                match result {
                    Ok(session) => session,
                    Err(error) => {
                        tracing::warn!(%error, database_id = ?lease_session.database_id, database = %lease_session.database_name, host = %manager.pg_client.host, port = manager.pg_client.port, "upstream connection failed");
                        reject_connection(framed, "08006", "unable to connect to PostgreSQL").await;
                        return None;
                    }
                }
            }
        };
        let remaining_stream = std::mem::take(framed.read_buffer_mut());
        let result = tokio::select! {
            biased;
            // Once response forwarding starts, close on cancellation without
            // appending an ErrorResponse to a possibly partial protocol frame.
            _ = cancellation.cancelled() => return None,
            result = session_relay::write_startup(
                framed.get_mut(), &mut session, &remaining_stream,
            ) => result,
        };
        if let Err(error) = result {
            tracing::debug!(%error, "unable to write client startup responses");
            return None;
        }
        Some(session)
    };
    // These comparable tails begin after attach/checkout and include sending
    // ReadyForQuery. The cold tail also establishes/authenticates its backend.
    let upstream_session = if is_warm {
        hotpath::measure_block!("connection::warm_startup_to_ready", startup.await)
    } else {
        hotpath::measure_block!("connection::cold_startup_to_ready", startup.await)
    }?;
    Some((upstream_session, lease_session))
}

#[cfg(test)]
mod warm_tests;

#[hotpath::measure]
async fn reject_connection(framed: &mut ClientConnection, code: &str, message: &str) {
    let error = ErrorInfo::new("FATAL".into(), code.into(), message.into());
    let _ = framed.send(PgWireBackendMessage::ErrorResponse(error.into())).await;
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConnectionFieldError {
    #[error("expected template/lease-id")]
    InvalidFormat,
    #[error(transparent)]
    InvalidLeaseId(#[from] InvalidLeaseId),
}

#[hotpath::measure]
pub fn parse_connection_field(input: &str) -> Result<(&str, LeaseId), ConnectionFieldError> {
    let (database, lease) = input.split_once('/').ok_or(ConnectionFieldError::InvalidFormat)?;
    if database.is_empty() || lease.contains('/') {
        return Err(ConnectionFieldError::InvalidFormat);
    }
    Ok((database, LeaseId::new(lease)?))
}

#[cfg(test)]
mod tests {
    #[test]
    fn connection_field_validates_lease_ids() {
        assert!(matches!(
            super::parse_connection_field("template"),
            Err(super::ConnectionFieldError::InvalidFormat)
        ));
        assert!(matches!(
            super::parse_connection_field("template/"),
            Err(super::ConnectionFieldError::InvalidLeaseId(_))
        ));
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
}
