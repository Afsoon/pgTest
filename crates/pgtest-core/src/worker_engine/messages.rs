use std::time::Instant;

use tokio_util::sync::CancellationToken;

use crate::{
    utils::ReadString,
    worker_engine::{
        core::{LeaseId, SlotIdx},
        errors::{AttachError, PostgresDDLClientError, ReleaseError},
        traits::ConsumerIO,
    },
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
    TemplateCreated {
        index: SlotIdx,
        result: Result<ReadString, PostgresDDLClientError>,
    },
    Detach {
        lease: LeaseId,
        generation: u64,
    },
    LeaseMaxTimeReached {
        lease: LeaseId,
        generation: u64,
    },
    DeleteLease {
        lease: LeaseId,
        generation: u64,
    },
    RetryDatabaseCreation {
        index: SlotIdx,
        try_number: usize,
    },
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

pub enum EngineMetricMessage {}
