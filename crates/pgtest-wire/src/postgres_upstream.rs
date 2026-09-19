use std::collections::BTreeMap;

use bytes::BytesMut;
use pgwire::messages::{
    DecodeContext, Message,
    startup::{Authentication, Startup},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::postgres_upstream::upstream_stream::UpstreamStream;

pub(crate) mod upstream_stream;

pub(crate) struct PostgresUpstream;

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

pub(crate) struct UpstreamSession {
    pub(crate) stream: UpstreamStream,
    pub(crate) session_burst: RawBytes,
}

#[hotpath::measure_all]
impl PostgresUpstream {
    pub(crate) async fn connect(
        db_name: &str,
        client_params: &BTreeMap<String, String>,
        pg_upstream_host: &str,
        pg_upstream_port: u16,
    ) -> Result<UpstreamSession, ()> {
        let mut stream = Self::connect_stream(pg_upstream_host, pg_upstream_port).await?;
        let mut decode_buffer = Self::authenticate(&mut stream, db_name, client_params).await?;

        let Ok(session_burst) =
            Self::wait_for_ready_for_query(&mut stream, &mut decode_buffer).await
        else {
            tracing::error!("unable to read the opaque rfq opaque");
            return Err(());
        };

        Ok(UpstreamSession { stream, session_burst })
    }

    async fn connect_stream(host: &str, port: u16) -> Result<UpstreamStream, ()> {
        if host.starts_with('/') {
            #[cfg(unix)]
            {
                let path = std::path::Path::new(host).join(format!(".s.PGSQL.{port}"));
                return tokio::net::UnixStream::connect(&path).await.map(UpstreamStream::Unix).map_err(|error| {
                    tracing::error!(path = %path.display(), %error, "unable to connect upstream postgres socket");
                });
            }
            #[cfg(not(unix))]
            {
                tracing::error!(
                    host,
                    port,
                    "Unix upstream sockets are unsupported on this platform"
                );
                return Err(());
            }
        }
        Self::connect_tcp(host, port).await.map(UpstreamStream::Tcp)
    }

    async fn connect_tcp(host: &str, port: u16) -> Result<TcpStream, ()> {
        let stream = TcpStream::connect((host, port)).await.map_err(|error| {
            tracing::error!(host, port, %error, "unable to connect upstream postgres");
        })?;
        stream.set_nodelay(true).map_err(|error| {
            tracing::error!(host, port, %error, "unable to set upstream TCP_NODELAY");
        })?;
        Ok(stream)
    }

    // Includes sending Startup and waiting for AuthenticationOk. Preserve any
    // following bytes so the next stage can consume an already-buffered reply.
    async fn authenticate(
        stream: &mut UpstreamStream,
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
        stream: &mut UpstreamStream,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tcp_uses_configured_hostname_and_port() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut stream = PostgresUpstream::connect_stream("localhost", port).await.unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        assert!(matches!(&stream, UpstreamStream::Tcp(tcp) if tcp.nodelay().unwrap()));
        stream.write_all(b"request").await.unwrap();
        let mut request = [0; 7];
        peer.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"request");
        peer.write_all(b"reply").await.unwrap();
        let mut reply = [0; 5];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"reply");
        stream.shutdown().await.unwrap();
        assert_eq!(peer.read(&mut request).await.unwrap(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_uses_directory_and_port_and_reports_missing_socket() {
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let directory =
            Directory(std::env::temp_dir().join(format!("pgu-up-{}", std::process::id())));
        std::fs::create_dir(&directory.0).unwrap();
        let host = directory.0.to_str().unwrap();
        let listener = crate::unix_listener::BoundUnixListener::bind(&directory.0, 5433).unwrap();
        let mut stream = PostgresUpstream::connect_stream(host, 5433).await.unwrap();
        assert!(matches!(stream, UpstreamStream::Unix(_)));
        let mut peer = listener.accept().await.unwrap();
        stream.write_all(b"request").await.unwrap();
        stream.flush().await.unwrap();
        let mut request = [0; 7];
        peer.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"request");
        peer.write_all(b"reply").await.unwrap();
        let mut reply = [0; 5];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"reply");
        stream.shutdown().await.unwrap();
        assert_eq!(peer.read(&mut request).await.unwrap(), 0);
        assert!(PostgresUpstream::connect_stream(host, 5434).await.is_err());
        drop(listener);
        assert!(PostgresUpstream::connect_stream(host, 5433).await.is_err());
    }

    #[cfg(not(unix))]
    #[tokio::test]
    async fn unix_endpoint_is_rejected_on_unsupported_platforms() {
        assert!(PostgresUpstream::connect_stream("/var/run/postgresql", 5432).await.is_err());
    }
}
