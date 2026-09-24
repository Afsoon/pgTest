use super::*;

#[derive(Debug, thiserror::Error)]
pub(crate) enum WarmShutdownError {
    #[error("cannot confirm warm pool shutdown: state lock poisoned")]
    StatePoisoned,
}

impl ConnectionWarmPool {
    /// Cancel all warm work and wait for resource disposal and scheduler exit.
    ///
    /// The caller must keep polling any caller-owned scheduler/attempt futures
    /// (or drop them). Cancelling this wait leaves shutdown in effect; another
    /// caller can resume waiting. Client-owned sessions are excluded.
    pub(crate) async fn shutdown(&self) -> Result<(), WarmShutdownError> {
        let drains = self.begin_shutdown();
        // Even if state is poisoned, cancellation still reaches the scheduler
        // and we wait for its owned attempt futures to be dropped.
        self.scheduler_drain.wait().await;
        for drain in drains? {
            drain.wait().await;
        }
        Ok(())
    }

    fn begin_shutdown(&self) -> Result<Vec<TaskTracker>, WarmShutdownError> {
        // Also cancel on the poisoned-state path. Admission checks this token
        // under the same mutex used for the snapshot below.
        self.cancellation.cancel();
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => {
                // A poisoned lock prevents any new scheduler admission.
                self.scheduler_drain.close();
                return Err(WarmShutdownError::StatePoisoned);
            }
        };
        self.scheduler_drain.close();
        let idle_count: usize = state.databases.values().map(|entry| entry.idle.len()).sum();
        let remaining_capacity = state
            .capacity_used
            .checked_sub(idle_count)
            .expect("idle sessions must occupy capacity");
        let mut idle = Vec::with_capacity(idle_count);
        let mut drains = Vec::with_capacity(state.databases.len());
        for entry in state.databases.values_mut() {
            entry.retiring = true;
            entry.retry_at = None;
            entry.drain.close();
            drains.push(entry.drain.clone());
            idle.extend(entry.idle.drain(..));
        }
        state.schedule_order.clear();
        state.capacity_used = remaining_capacity;
        drop(state);
        // Socket tokens remain live until these sockets actually close. Other
        // shutdown/drain callers cannot mistake inventory removal for disposal.
        drop(idle);
        self.changed.notify_one();
        Ok(drains)
    }
}
