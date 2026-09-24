use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, ReadBuf};

use super::{scheduler::WarmSchedulerError, *};

impl ConnectionWarmPool {
    /// Register the scheduler's waker on idle sockets and evict one unhealthy
    /// spare. No socket references survive the state lock, so checkout and
    /// retirement cannot race with a monitor reading client-owned traffic.
    pub(super) fn poll_idle_health(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WarmSchedulerError>> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(WarmSchedulerError::StatePoisoned)),
        };
        let current_capacity = state.capacity_used;
        for entry in state.databases.values_mut() {
            let unhealthy = entry.idle.iter_mut().position(|session| {
                // One nonblocking read only. Any data (including a partial
                // ErrorResponse or an unsolicited Notice) conservatively
                // discards this unused backend instead of replaying it later.
                let mut byte = [0];
                let mut buffer = ReadBuf::new(&mut byte);
                Pin::new(&mut session.session.stream).poll_read(cx, &mut buffer).is_ready()
            });
            if let Some(index) = unhealthy {
                let remaining_capacity =
                    current_capacity.checked_sub(1).expect("idle session must occupy capacity");
                let session = entry.idle.remove(index).expect("idle session must exist");
                state.capacity_used = remaining_capacity;
                drop(state);
                drop(session);
                self.changed.notify_one();
                return Poll::Ready(Ok(()));
            }
        }
        Poll::Pending
    }
}
