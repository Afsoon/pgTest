use super::*;

/// Result of the optional initial warm-up. Every outcome permits cold fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmStartupOutcome {
    Disabled,
    BackgroundOnly,
    Ready { connections: usize },
    TimedOut { ready: usize, target: usize },
    Stopped,
}

impl ConnectionWarmPool {
    pub(super) async fn wait_initial(&self) -> WarmStartupOutcome {
        if !self.config.is_enabled() {
            return WarmStartupOutcome::Disabled;
        }
        if self.config.startup_wait.is_zero() {
            return WarmStartupOutcome::BackgroundOnly;
        }
        // Freeze the initial population: runtime creation never extends
        // startup.
        let initial: Vec<_> = match self.state.lock() {
            Ok(state) => state
                .databases
                .iter()
                .filter(|(_, entry)| !entry.retiring)
                .map(|(id, _)| *id)
                .collect(),
            Err(_) => return WarmStartupOutcome::Stopped,
        };
        let target = initial
            .len()
            .saturating_mul(usize::from(self.config.per_database))
            .min(self.config.max_total);
        let deadline = tokio::time::sleep(self.config.startup_wait);
        tokio::pin!(deadline);
        loop {
            // Register before observing inventory so a publication between the
            // observation and await cannot be missed. This also allows waiters
            // to coexist with the replenishment scheduler.
            let changed = self.startup_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.cancellation.is_cancelled() {
                return WarmStartupOutcome::Stopped;
            }
            let ready = match self.state.lock() {
                Ok(state) => initial
                    .iter()
                    .filter_map(|id| state.databases.get(id))
                    .filter(|entry| !entry.retiring)
                    .map(|entry| entry.idle.len())
                    .sum(),
                Err(_) => return WarmStartupOutcome::Stopped,
            };
            if ready >= target {
                return WarmStartupOutcome::Ready { connections: ready };
            }
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return WarmStartupOutcome::Stopped,
                _ = &mut deadline => return WarmStartupOutcome::TimedOut { ready, target },
                _ = changed => {},
            }
        }
    }
}
