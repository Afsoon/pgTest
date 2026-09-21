use std::time::Instant;

use pgtest_utils::read_string::ReadString;
use tokio_util::sync::CancellationToken;

use crate::worker_engine::{
    core::LeaseId,
    database_jobs::DatabaseWorkerMessages,
    errors::{AttachError, ReleaseError},
    traits::ConsumerIO,
};

#[cfg_attr(test, derive(Debug))]
pub enum EngineMessage<C: ConsumerIO> {
    AttachOrJoin {
        lease: LeaseId,
        reply: C,
        message_time: Instant,
    },
    ReleaseLease {
        lease: LeaseId,
        reply: C,
    },
    Detach {
        lease: LeaseId,
        generation: u64,
    },
    LeaseMaxTimeReached {
        lease: LeaseId,
        generation: u64,
    },
    DatabaseWorker(DatabaseWorkerMessages),
    #[cfg(test)]
    Barrier {
        reply: tokio::sync::oneshot::Sender<()>,
    },
    Shutdown,
}

#[cfg_attr(test, derive(Clone, Debug))]
pub enum ConsumerReply {
    Attached { database_name: ReadString, generation: u64, cancellation: CancellationToken },
    FailedToAttach,
    AttachRejected(AttachError),
    ReleaseResult(Result<(), ReleaseError>),
}
