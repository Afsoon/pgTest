use std::{
    io,
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
};

use tokio::{
    net::{UnixListener, UnixStream},
    sync::oneshot,
    task::JoinHandle,
};

/// Owns the socket file as well as the listener. Never unlinks an existing path
/// on bind, or a replacement file on drop.
pub(crate) struct BoundUnixListener {
    listener: UnixListener,
    socket: SocketFile,
}

struct SocketFile {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl BoundUnixListener {
    pub(crate) fn bind(directory: &Path, port: u16) -> io::Result<Self> {
        let path = directory.canonicalize()?.join(format!(".s.PGSQL.{port}"));
        let listener = UnixListener::bind(&path)?;
        let metadata = std::fs::symlink_metadata(&path)?;
        let socket = SocketFile { path, device: metadata.dev(), inode: metadata.ino() };
        Ok(Self { listener, socket })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.socket.path
    }

    pub(crate) async fn accept(&self) -> io::Result<UnixStream> {
        self.listener.accept().await.map(|(stream, _)| stream)
    }
}

impl Drop for SocketFile {
    fn drop(&mut self) {
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path) {
            if metadata.file_type().is_socket()
                && metadata.dev() == self.device
                && metadata.ino() == self.inode
            {
                if let Err(error) = std::fs::remove_file(&self.path) {
                    tracing::warn!(path = %self.path.display(), %error, "unable to remove Unix socket");
                }
            }
        }
    }
}

/// Keep this handle alive while accepting Unix connections. Explicit shutdown
/// waits for the accept task to drop its listener and socket-file guard.
pub struct UnixWireListener {
    pub(crate) task: Option<JoinHandle<()>>,
    pub(crate) stop: Option<oneshot::Sender<()>>,
    pub(crate) path: PathBuf,
}

impl UnixWireListener {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for UnixWireListener {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "pgu-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn failed_second_bind_preserves_the_live_socket() {
        let directory = TestDirectory::new();
        let listener = BoundUnixListener::bind(&directory.0, 6432).unwrap();
        let path = listener.path().to_owned();
        assert!(BoundUnixListener::bind(&directory.0, 6432).is_err());
        let client = UnixStream::connect(&path).await.unwrap();
        let server = listener.accept().await.unwrap();
        drop((client, server, listener));
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn existing_regular_file_is_not_overwritten() {
        let directory = TestDirectory::new();
        let path = directory.0.join(".s.PGSQL.6432");
        std::fs::write(&path, "keep this file").unwrap();
        assert!(BoundUnixListener::bind(&directory.0, 6432).is_err());
        assert_eq!(std::fs::read_to_string(path).unwrap(), "keep this file");
    }

    #[tokio::test]
    async fn dropping_listener_preserves_a_replacement_path() {
        let directory = TestDirectory::new();
        let listener = BoundUnixListener::bind(&directory.0, 6432).unwrap();
        let path = listener.path().to_owned();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, "replacement").unwrap();
        drop(listener);
        assert_eq!(std::fs::read_to_string(path).unwrap(), "replacement");
    }
}
