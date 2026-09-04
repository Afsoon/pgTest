use thiserror::Error;

use crate::worker_engine::core::{LeaseId, SlotIdx};

#[derive(Error, Debug)]
pub enum LeaseSlotError {
    #[error("worker slot {0} is not initialized")]
    WorkerSlotNotInitialized(SlotIdx),
    #[error("worker slot {slot} is in state {state}, expected Ready")]
    WorkerSlotNotReady { slot: SlotIdx, state: &'static str },
}

#[derive(Error, Debug)]
pub enum OfferTemplateError {
    #[error("worker slot {0} is not initialized")]
    WorkerSlotNotInitialized(SlotIdx),
}

#[derive(Error, Debug)]
pub enum ForceRecycleError {
    #[error("no lease registered for id {0}")]
    UnknownLease(LeaseId),
    #[error("worker slot {0} is not initialized")]
    WorkerSlotNotInitialized(SlotIdx),
    #[error("lost connectivity with the postgres server")]
    DatabaseIsDown,
}

#[derive(Error, Debug)]
pub enum RecycleError {
    #[error("no lease registered for id {0}")]
    UnknownLease(LeaseId),
    #[error("lease {0} still has open connections")]
    ConnectionNotClosedYet(LeaseId),
    #[error("worker slot {0} is not initialized")]
    WorkerSlotNotInitialized(SlotIdx),
    #[error("lost connectivity with the postgres server")]
    DatabaseIsDown,
}

#[derive(Error, Debug)]
pub enum IOError {
    #[error("failed to deliver a message to the worker engine (channel closed)")]
    FailedToSendTheMessage,
    #[error("failed to spawn the background database-creation task for slot {0}")]
    FailedToStartABackgroundProcess(SlotIdx),
}

#[derive(Error, Debug)]
pub enum MetricIOError {
    #[error("failed to deliver the metric message to the worker")]
    FailedToSendMetricMessage,
}

#[derive(Error, Debug)]
pub enum ConsumerIOError {
    #[error("failed to reply to the consumer; its reply channel is gone")]
    FailedToReplyTheConsumer,
}

#[derive(Error, Debug)]
pub enum PostgresDDLClientError {
    #[error("an unexpected error happened trying to {0}")]
    NonRecoverableError(String),
    #[error("unable to {operation} after {retries}")]
    OperationNotExecutedAfterCertainRetries { operation: String, retries: usize },
}
