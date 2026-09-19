//! Byte forwarding and cancellation for an attached lease session.

use bytes::BytesMut;
use pgtest::worker_manager::worker_io::LeaseSession;
use pgwire::tokio::server::MaybeTls;
use tokio::io::{AsyncWriteExt, copy_bidirectional_with_sizes};

use crate::postgres_upstream::UpstreamSession;

pub(crate) struct SessionRelay;

const RELAY_BUF: usize = 16 * 1024;

#[hotpath::measure_all]
impl SessionRelay {
    pub(crate) async fn run(
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
