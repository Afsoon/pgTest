use std::time::Instant;

use crate::{
    utils::ReadString,
    worker_engine::{
        core::{LeaseId, SlotIdx},
        errors::PostgresDDLClientError,
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
    TemplateCreated {
        index: SlotIdx,
        result: Result<ReadString, PostgresDDLClientError>,
    },
    Detach {
        lease: LeaseId,
    },
    GraceExpired {
        lease: LeaseId,
    },
    LeaseMaxTimeReached {
        lease: LeaseId,
    },
    DeleteLease {
        lease: LeaseId,
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
    Attached { database_name: ReadString },
    FailedToAttach,
}

pub enum EngineMetricMessage {}
