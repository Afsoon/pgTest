use pgtest_utils::read_string::ReadString;
use tokio_util::sync::CancellationToken;

use crate::worker_engine::{
    database_jobs::{CleanupDatabase, CreateDatabases},
    errors::{IOError, PostgresDDLClientError},
    messages::{ConsumerReply, EngineMessage},
};

pub trait ConsumerIO: Send + 'static {
    fn reply(self, msg: ConsumerReply) -> Result<(), ConsumerReply>;
}

pub trait EngineIO<C: ConsumerIO> {
    fn request_creation(&self, request: CreateDatabases) -> Result<(), IOError>;
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
    /// Report one final result per index in `0..amount`; cancellation stops
    /// delivery.
    fn create_databases(
        &self,
        amount: usize,
        on_finished: impl FnMut(usize, Result<ReadString, PostgresDDLClientError>) + Send,
    ) -> impl Future<Output = ()> + Send;
    fn create_database(
        &self,
    ) -> impl Future<Output = Result<ReadString, PostgresDDLClientError>> + Send;
    fn drop_database(
        &self,
        database_name: &str,
    ) -> impl Future<Output = Result<(), PostgresDDLClientError>> + Send;
    /// Report exactly one final result per input index, as each drop finishes.
    /// Cancelling the returned future cancels delivery of remaining results.
    fn drop_databases(
        &self,
        database_names: &[ReadString],
        on_finished: impl FnMut(usize, Result<(), PostgresDDLClientError>) + Send,
    ) -> impl Future<Output = ()> + Send;
}
