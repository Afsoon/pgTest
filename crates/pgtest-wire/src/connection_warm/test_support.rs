use bytes::BytesMut;
use tokio::io::AsyncReadExt;

use super::*;
use crate::postgres_upstream::upstream_stream::UpstreamStream;

// ParameterStatus, BackendKeyData, and idle ReadyForQuery from a completed
// startup.
pub(super) const BURST: &[u8] = b"S\0\0\0\x08a\0b\0K\0\0\0\x0c\0\0\0\x07\0\0\0\x09Z\0\0\0\x05I";
pub(super) const OTHER_BURST: &[u8] = b"S\0\0\0\x08x\0y\0Z\0\0\0\x05I";

pub(super) async fn ready_session(burst: &[u8]) -> (UpstreamSession, UpstreamStream) {
    #[cfg(unix)]
    let (stream, peer) = {
        let (stream, peer) = tokio::net::UnixStream::pair().unwrap();
        (UpstreamStream::Unix(stream), UpstreamStream::Unix(peer))
    };
    #[cfg(not(unix))]
    let (stream, peer) = {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            let (stream, accepted) =
                tokio::join!(tokio::net::TcpStream::connect(address), listener.accept());
            (UpstreamStream::Tcp(stream.unwrap()), UpstreamStream::Tcp(accepted.unwrap().0))
        })
        .await
        .expect("local socket pair must connect")
    };
    // No PostgreSQL is involved: publication receives an already-ready session.
    (UpstreamSession { stream, session_burst: BytesMut::from(burst).into() }, peer)
}

pub(super) async fn assert_peer_closed(mut peer: UpstreamStream) {
    let mut byte = [0];
    let count = tokio::time::timeout(Duration::from_secs(5), peer.read(&mut byte))
        .await
        .expect("dropped session must close its socket")
        .unwrap();
    assert_eq!(count, 0);
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct DatabaseSnapshot {
    pub(super) name: String,
    pub(super) in_flight: usize,
    pub(super) bursts: Vec<Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct PoolSnapshot {
    pub(super) capacity_used: usize,
    pub(super) databases: BTreeMap<u64, DatabaseSnapshot>,
}

pub(super) fn snapshot(pool: &ConnectionWarmPool) -> PoolSnapshot {
    // Tests alone inspect poisoned state to detect partial changes after a
    // caught invariant panic. Production operations must continue to reject it.
    let state = pool.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    PoolSnapshot {
        capacity_used: state.capacity_used,
        databases: state
            .databases
            .iter()
            .map(|(id, entry)| {
                (
                    id.0,
                    DatabaseSnapshot {
                        name: entry.database_name.clone(),
                        in_flight: entry.in_flight,
                        bursts: entry
                            .idle
                            .iter()
                            .map(|idle| idle.session.session_burst.bytes().to_vec())
                            .collect(),
                    },
                )
            })
            .collect(),
    }
}
