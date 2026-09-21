use pgtest_database_operations::manager::{PostgresManager, errors::PostgresOperationsError};
use pgtest_utils::read_string::ReadString;
use tokio_retry2::{
    Retry,
    strategy::{ExponentialFactorBackoff, jitter},
};

use crate::worker_engine::{errors::PostgresDDLClientError, traits::PostgresClient};

#[hotpath::measure_all]
impl PostgresClient for PostgresManager {
    async fn drop_database(&self, database_name: &str) -> Result<(), PostgresDDLClientError> {
        let retry_strategy = ExponentialFactorBackoff::from_millis(250, 1.0).map(jitter).take(3);

        match Retry::spawn(retry_strategy, || self.drop_ddl_database(database_name)).await {
            Ok(()) => Ok(()),
            Err(PostgresOperationsError::NonTransientError { .. }) => {
                let message_error = format!("drop the database {database_name}");
                Err(PostgresDDLClientError::NonRecoverableError(message_error))
            }
            Err(_) => {
                let message_error = format!("drop the database {database_name}");
                Err(PostgresDDLClientError::OperationNotExecutedAfterCertainRetries {
                    operation: message_error,
                    retries: 3,
                })
            }
        }
    }

    async fn drop_templates_like(&self) -> Result<(), PostgresDDLClientError> {
        let retry_strategy = ExponentialFactorBackoff::from_millis(250, 1.0).map(jitter).take(3);
        match Retry::spawn(retry_strategy, || self.drop_ddl_templates_like()).await {
            Ok(()) => Ok(()),
            Err(PostgresOperationsError::NonTransientError { .. }) => {
                let message_error = format!("drop all database except the template");
                Err(PostgresDDLClientError::NonRecoverableError(message_error))
            }
            Err(_) => {
                let message_error = format!("drop all database except the template");
                Err(PostgresDDLClientError::OperationNotExecutedAfterCertainRetries {
                    operation: message_error,
                    retries: 3,
                })
            }
        }
    }

    async fn create_database(&self) -> Result<ReadString, PostgresDDLClientError> {
        let retry_strategy = ExponentialFactorBackoff::from_millis(250, 1.0).map(jitter).take(3);
        match Retry::spawn(retry_strategy, || self.create_ddl_database()).await {
            Ok(database_name) => Ok(ReadString::from(database_name)),
            Err(PostgresOperationsError::NonTransientError { database_name, .. }) => {
                let message_error =
                    format!("unable to create the database {database_name} from the template");
                Err(PostgresDDLClientError::NonRecoverableError(message_error))
            }
            Err(PostgresOperationsError::UnableToCreateDatabase { database_name, .. }) => {
                let message_error = format!(
                    "unable create the database {database_name} from the template. Retrying again"
                );
                Err(PostgresDDLClientError::OperationNotExecutedAfterCertainRetries {
                    operation: message_error,
                    retries: 3,
                })
            }
            Err(_) => Err(PostgresDDLClientError::NonRecoverableError(String::from(
                "Unexpected error found creating the database",
            ))),
        }
    }
}
