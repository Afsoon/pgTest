use std::{
    future::{pending, poll_fn},
    sync::atomic::Ordering,
};

use futures::{FutureExt, StreamExt, stream::FuturesUnordered};

use super::*;

#[derive(Debug, thiserror::Error)]
pub(crate) enum WarmSchedulerError {
    #[error("a warm scheduler is already running for this pool")]
    AlreadyRunning,
    #[error("warm pool state lock poisoned")]
    StatePoisoned,
    #[error("warm connection concurrency limiter closed")]
    ConcurrencyClosed,
}

struct SchedulerGuard<'a>(&'a ConnectionWarmPool);

impl Drop for SchedulerGuard<'_> {
    fn drop(&mut self) {
        self.0.scheduler_running.store(false, Ordering::Release);
    }
}

impl ConnectionWarmPool {
    /// Drive bounded replenishment until pool cancellation or a terminal error.
    ///
    /// The caller owns this future; no attempt tasks are detached. Returning or
    /// dropping it releases all pending attempt resources. Published idle
    /// sessions remain owned by the pool for later checkout/retirement.
    pub(crate) async fn run_scheduler(
        self: Arc<Self>,
        upstream_host: String,
        upstream_port: u16,
    ) -> Result<(), WarmSchedulerError> {
        if !self.config.is_enabled() || self.cancellation.is_cancelled() {
            return Ok(());
        }
        self.scheduler_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| WarmSchedulerError::AlreadyRunning)?;
        let _guard = SchedulerGuard(&self);
        // Drop attempts before releasing the single-scheduler guard, including
        // when this future is aborted by its caller.
        let mut attempts = FuturesUnordered::new();
        let limit = self.config.attempt_limit();
        let host = upstream_host.as_str();
        let cancellation = &self.cancellation;

        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            if self.attempt_permits.is_closed() {
                return Err(WarmSchedulerError::ConcurrencyClosed);
            }
            if self.state.is_poisoned() {
                return Err(WarmSchedulerError::StatePoisoned);
            }

            // Attempt completion already updates retry state and capacity.
            // Remove ready results before deciding how much new work fits.
            while let Some(Some((database_id, result))) = attempts.next().now_or_never() {
                handle_completion(database_id, result)?;
            }
            while attempts.len() < limit {
                let Some(reservation) = self.reserve_next() else {
                    break;
                };
                attempts.push(async move {
                    let database_id = reservation.database_id;
                    let result = reservation.establish(host, upstream_port, cancellation).await;
                    (database_id, result)
                });
            }

            let deadline = self.next_retry_deadline(attempts.len() < limit)?;
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                Some((database_id, result)) = attempts.next(), if !attempts.is_empty() => {
                    handle_completion(database_id, result)?;
                }
                result = poll_fn(|cx| self.poll_idle_health(cx)) => result?,
                // notify_one retains a permit when no waiter is registered:
                // changes between state inspection and this wait are not lost.
                _ = self.changed.notified() => {}
                _ = wait_for_retry(deadline) => {}
            }
        }
    }

    fn next_retry_deadline(
        &self,
        has_attempt_slot: bool,
    ) -> Result<Option<Instant>, WarmSchedulerError> {
        if !has_attempt_slot {
            return Ok(None);
        }
        let state = self.state.lock().map_err(|_| WarmSchedulerError::StatePoisoned)?;
        if state.capacity_used >= self.config.max_total {
            return Ok(None);
        }
        Ok(state
            .databases
            .values()
            .filter(|entry| {
                !entry.retiring
                    && !entry.cancellation.is_cancelled()
                    && entry.idle.len() + entry.in_flight < usize::from(self.config.per_database)
            })
            .filter_map(|entry| entry.retry_at)
            .min())
        // Keep a due deadline if its entry is otherwise eligible: it may have
        // elapsed between reserve_next and this inspection. One immediate wake
        // will reserve it. Entries blocked by other limits get no retry timer.
    }
}

fn handle_completion(
    database_id: DatabaseId,
    result: Result<bool, WarmAttemptError>,
) -> Result<(), WarmSchedulerError> {
    match result {
        Err(WarmAttemptError::ConcurrencyClosed) => Err(WarmSchedulerError::ConcurrencyClosed),
        Err(error @ (WarmAttemptError::TimedOut | WarmAttemptError::Upstream(_))) => {
            tracing::warn!(?database_id, %error, "warm connection attempt failed; retry deferred");
            Ok(())
        }
        Ok(_) | Err(WarmAttemptError::Cancelled) => Ok(()),
    }
}

async fn wait_for_retry(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => pending().await,
    }
}
