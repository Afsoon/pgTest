use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::{
    connection_warm::WarmStartupProfile,
    postgres_upstream::{self, UpstreamError, UpstreamSession},
};

const WARM_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub(crate) enum WarmAttemptError {
    #[error("warm connection attempt cancelled")]
    Cancelled,
    #[error("warm connection timed out")]
    TimedOut,
    #[error("warm connection concurrency limiter closed")]
    ConcurrencyClosed,
    #[error(transparent)]
    Upstream(#[from] UpstreamError),
}

pub(super) async fn connect_warm_session(
    database_name: &str,
    profile: &WarmStartupProfile,
    upstream_host: &str,
    upstream_port: u16,
    cancellation: &CancellationToken,
) -> Result<UpstreamSession, WarmAttemptError> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(WarmAttemptError::Cancelled),
        connection_result = async {
            hotpath::measure_block!("connection_warm::upstream_startup", {
                tokio::time::timeout(WARM_ATTEMPT_TIMEOUT, postgres_upstream::connect(
                    database_name,
                    profile.parameters(),
                    upstream_host,
                    upstream_port,
                )).await
            })
        } => {
            match connection_result {
                Ok(Ok(session)) => Ok(session),
                Ok(Err(error)) => Err(WarmAttemptError::Upstream(error)),
                Err(_) => Err(WarmAttemptError::TimedOut)
            }
        }
    }
}
