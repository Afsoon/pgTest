use pgtest::{
    worker_engine::lifecycle::{DatabaseLifecycle, NoopDatabaseLifecycle},
    worker_manager::WorkerEngineManager,
};
use tokio::task::JoinHandle;

use super::{scheduler::WarmSchedulerError, *};
use crate::wire_listener::{self, TcpWireListener, WireError};

/// Owns one warm pool shared by lifecycle callbacks and all client listeners.
///
/// Install `lifecycle()` before starting the manager, then call `start()`
/// before opening listeners. Stop listeners, call `shutdown()`, then stop the
/// manager. Dropping this owner cancels warming and aborts its scheduler,
/// including when startup is interrupted. The default configuration creates no
/// pool or task.
pub struct ConnectionWarmer {
    pool: Option<Arc<ConnectionWarmPool>>,
    task: Option<JoinHandle<Result<(), WarmSchedulerError>>>,
}

impl ConnectionWarmer {
    pub fn new(config: ConnectionWarmConfig, default_user: &str) -> Self {
        Self {
            pool: config
                .is_enabled()
                .then(|| Arc::new(ConnectionWarmPool::new(config, default_user))),
            task: None,
        }
    }

    pub fn lifecycle(&self) -> Arc<dyn DatabaseLifecycle> {
        match &self.pool {
            Some(pool) => pool.clone(),
            None => Arc::new(NoopDatabaseLifecycle),
        }
    }

    /// Start background replenishment and optionally wait for initial
    /// inventory. Call once after the manager has created its initial
    /// databases. Warm-up errors and deadlines are logged and leave cold
    /// connections available.
    pub async fn start(&mut self, manager: &WorkerEngineManager) -> WarmStartupOutcome {
        let Some(pool) = &self.pool else {
            return WarmStartupOutcome::Disabled;
        };
        assert!(self.task.is_none(), "warm scheduler already started");
        self.task = Some(tokio::spawn(
            pool.clone().run_scheduler(manager.pg_client.host.clone(), manager.pg_client.port),
        ));
        let outcome = tokio::select! {
            biased;
            result = self.task.as_mut().unwrap() => {
                tracing::warn!(?result, "warm scheduler stopped during startup; using cold fallback");
                self.task.take();
                WarmStartupOutcome::Stopped
            }
            outcome = pool.wait_initial() => outcome,
        };
        match outcome {
            WarmStartupOutcome::TimedOut { ready, target } => {
                tracing::warn!(
                    ready,
                    target,
                    "initial connection warm-up incomplete; continuing with cold fallback"
                );
            }
            WarmStartupOutcome::Stopped => {
                tracing::warn!("initial connection warm-up stopped; continuing with cold fallback");
            }
            _ => tracing::debug!(?outcome, "initial connection warm-up finished"),
        }
        outcome
    }

    pub async fn listen_tcp(
        &self,
        manager: Arc<WorkerEngineManager>,
        address: std::net::SocketAddr,
    ) -> Result<TcpWireListener, WireError> {
        wire_listener::run_with_pool(manager, address, self.pool.clone()).await
    }

    #[cfg(unix)]
    pub async fn listen_unix(
        &self,
        manager: Arc<WorkerEngineManager>,
        directory: &std::path::Path,
        port: u16,
    ) -> Result<wire_listener::UnixWireListener, WireError> {
        wire_listener::run_unix_with_pool(manager, directory, port, self.pool.clone()).await
    }

    pub async fn shutdown(&mut self) -> Result<(), WarmShutdownError> {
        let result = match &self.pool {
            Some(pool) => pool.shutdown().await,
            None => Ok(()),
        };
        if let Some(task) = self.task.take() {
            match task.await {
                Ok(Ok(())) => {}
                error => tracing::warn!(?error, "warm scheduler stopped with an error"),
            }
        }
        result
    }
}

impl Drop for ConnectionWarmer {
    fn drop(&mut self) {
        if let Some(pool) = &self.pool {
            let _ = pool.begin_shutdown();
        }
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;
