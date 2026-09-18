use tokio_util::sync::CancellationToken;

use crate::{
    utils::ReadString,
    worker_engine::{
        database_jobs::{CleanupDatabase, CreateDatabase},
        errors::{IOError, MetricIOError, PostgresDDLClientError},
        messages::{ConsumerReply, EngineMessage, EngineMetricMessage},
    },
};

pub trait ConsumerIO: Send + 'static {
    fn reply(self, msg: ConsumerReply) -> Result<(), ConsumerReply>;
}

pub trait MetricIO {
    fn send_metric(
        &self,
        metric_message: EngineMetricMessage,
    ) -> impl Future<Output = Result<(), MetricIOError>> + Send;
}

pub trait EngineIO<C: ConsumerIO, P: PostgresClient> {
    fn request_creation(&self, request: CreateDatabase) -> Result<(), IOError>;
    fn request_cleanup(&self, request: CleanupDatabase) -> Result<(), IOError>;

    fn send_delayed_message(
        &self,
        msg: EngineMessage<C>,
        wait_duration: u32,
        cancel_token: CancellationToken,
    ) -> Result<(), IOError>;
    fn send_message(&self, msg: EngineMessage<C>) -> Result<(), IOError>;
}

pub trait EngineInbox<C: ConsumerIO> {
    fn wait_for_message(&mut self) -> impl Future<Output = Option<EngineMessage<C>>> + Send;
}

pub trait PostgresClient {
    fn create_database(
        &self,
    ) -> impl Future<Output = Result<ReadString, PostgresDDLClientError>> + Send;
    fn drop_database(
        &self,
        database_name: &str,
    ) -> impl Future<Output = Result<(), PostgresDDLClientError>> + Send;
    fn drop_templates_like(
        &self,
    ) -> impl Future<Output = Result<(), PostgresDDLClientError>> + Send;
}
