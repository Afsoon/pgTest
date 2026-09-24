use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use pgtest::worker_manager::WorkerEngineManager;
use pgtest_pg_wire::connection_warm::ConnectionWarmer;

use crate::args::ServeOptions;

pub async fn serve(options: ServeOptions) -> Result<()> {
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let postgres_config = options.postgres_config();
    let mut warmer =
        ConnectionWarmer::new(options.connection_warm_config()?, &postgres_config.pgtest_pg_user);
    let startup = WorkerEngineManager::start_with_lifecycle(
        postgres_config,
        options.engine_config(),
        warmer.lifecycle(),
    );
    #[cfg(unix)]
    let engine = tokio::select! {
        result = startup => result.context("failed to start database engine")?,
        _ = interrupt.recv() => return Ok(()),
        _ = terminate.recv() => return Ok(()),
    };
    #[cfg(not(unix))]
    let engine = startup.await.context("failed to start database engine")?;
    let engine = Arc::new(engine);
    let mut tcp_listener = None;
    #[cfg(unix)]
    let mut unix_listener = None;

    let result: Result<()> = async {
        #[cfg(unix)]
        tokio::select! {
            _ = warmer.start(&engine) => {},
            _ = interrupt.recv() => return Ok(()),
            _ = terminate.recv() => return Ok(()),
        }
        #[cfg(not(unix))]
        warmer.start(&engine).await;
        if let Some(address) = options.tcp_address() {
            let listener = warmer
                .listen_tcp(engine.clone(), address)
                .await
                .context("failed to start TCP listener")?;
            tracing::info!(address = %listener.local_addr(), "pgtest TCP listening");
            tcp_listener = Some(listener);
        }
        #[cfg(unix)]
        if let Some(directory) = &options.unix_socket_dir {
            let listener = warmer
                .listen_unix(engine.clone(), directory, options.unix_socket_port.unwrap_or(6432))
                .await
                .context("failed to start Unix listener")?;
            tracing::info!(path = %listener.path().display(), "pgtest Unix listening");
            unix_listener = Some(listener);
        }
        #[cfg(unix)]
        tokio::select! {
            _ = interrupt.recv() => {},
            _ = terminate.recv() => {},
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await.context("failed to wait for Ctrl-C")?;
        Ok(())
    }
    .await;

    tracing::info!("Stopping pgtest server");
    if let Some(listener) = tcp_listener {
        listener.shutdown().await;
    }
    #[cfg(unix)]
    if let Some(listener) = unix_listener {
        listener.shutdown().await;
    }
    let warm_shutdown = warmer.shutdown().await.context("failed to drain warm connections");
    let engine = Arc::try_unwrap(engine)
        .map_err(|_| anyhow!("listeners retained the database engine after shutdown"))?;
    engine.shutdown().await;
    result.and(warm_shutdown)
}
