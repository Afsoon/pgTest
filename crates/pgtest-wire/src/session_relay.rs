use pgtest::worker_manager::worker_io::LeaseSession;
use pgwire::tokio::server::MaybeTls;
use tokio::io::{AsyncWriteExt, copy_bidirectional_with_sizes};

use crate::postgres_upstream::UpstreamSession;

const RELAY_BUF: usize = 16 * 1024;

pub(crate) async fn write_startup(
    client: &mut MaybeTls,
    session: &mut UpstreamSession,
    remaining_stream: &[u8],
) -> std::io::Result<()> {
    if !remaining_stream.is_empty() {
        session.stream.write_all(remaining_stream).await?;
    }
    client.write_all(session.session_burst.bytes()).await
}

#[hotpath::measure]
pub(crate) async fn run(
    upstream_client: MaybeTls,
    upstream_session: UpstreamSession,
    lease_session: LeaseSession,
) -> std::io::Result<()> {
    let mut upstream_client = hotpath::io!(upstream_client, label = "client-relay");
    let mut upstream_stream = hotpath::io!(upstream_session.stream, label = "postgres-relay");
    let cancellation = lease_session.cancellation_token();
    let relay = async {
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
        _ = cancellation.cancelled() => Ok(()),
        result = relay => result,
    }
}
