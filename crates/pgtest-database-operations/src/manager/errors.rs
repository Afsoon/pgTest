use std::error::Error as StdError;

use deadpool_postgres::PoolError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PostgresClientError {
    #[error("{0} must be greater than zero")]
    InvalidPoolSize(&'static str),
    #[error("unable to connect to postgres at {0}")]
    UnableToConnectToPostgres(String),
    #[error("unable to fetch the postgres server version")]
    UnableToFetchPostgresVersion,
    #[error("unable to fetch the list of available databases")]
    UnableToFetchDatabaseList,
    #[error("database {0} does not exist")]
    DatabaseDoesNotExist(String),
    #[error("unsupported postgres version {0}; expected version 13 or higher")]
    UnsupportedVersion(u8),
    #[error("unexpected server version format: got {0}, expected an unsigned integer like 130_000")]
    UnexpectedServerVersionFormatFetched(i64),
}

#[derive(Error, Debug)]
pub enum PostgresOperationsError {
    #[error("non-transient postgres failure on {database_name}: {source}")]
    NonTransientError {
        database_name: String,
        #[source]
        source: PoolError,
    },
    #[error("transient failure creating database {database_name}: {source}")]
    UnableToCreateDatabase {
        database_name: String,
        #[source]
        source: PoolError,
    },
    #[error("transient failure dropping database {database_name}: {source}")]
    UnableToDropDatabase {
        database_name: String,
        #[source]
        source: PoolError,
    },
    #[error("unable to list databases: {0}")]
    UnableToListDatabases(#[source] PoolError),
}

impl PostgresOperationsError {
    pub fn classify_create(database_name: String, source: PoolError) -> Self {
        if PostgresOperationsError::is_transient(&source) {
            PostgresOperationsError::UnableToCreateDatabase { database_name, source }
        } else {
            PostgresOperationsError::NonTransientError { database_name, source }
        }
    }

    pub fn classify_drop(database_name: String, source: PoolError) -> Self {
        if PostgresOperationsError::is_transient(&source) {
            PostgresOperationsError::UnableToDropDatabase { database_name, source }
        } else {
            PostgresOperationsError::NonTransientError { database_name, source }
        }
    }

    /// Whether a failed postgres operation is worth retrying.
    ///
    /// Transient: connection loss, pool timeouts, connectivity blips (class
    /// 08), server restarting (57P*), resource pressure that clears (53200
    /// out of memory, 53300 too many connections) and busy objects (55006
    /// template in use, 55P03 lock not available). Everything else is
    /// non-transient by default: 42P04 duplicate database, 3D000 missing
    /// template, 53100 disk full, privilege errors, unknown codes,
    /// decode/config errors, a closed pool.
    fn is_transient(err: &PoolError) -> bool {
        match err {
            PoolError::Timeout(_) => true,
            PoolError::Backend(error) => {
                if let Some(code) = error.code() {
                    return Self::is_transient_code(code.code());
                }
                if error.is_closed() {
                    return true;
                }
                let mut source = error.source();
                while let Some(cause) = source {
                    if cause.is::<std::io::Error>() {
                        return true;
                    }
                    source = cause.source();
                }
                false
            }
            _ => false,
        }
    }

    fn is_transient_code(code: &str) -> bool {
        code.starts_with("08")
            || code.starts_with("57P")
            || matches!(code, "53200" | "53300" | "55006" | "55P03")
    }
}

#[cfg(test)]
mod tests {
    use deadpool_postgres::{PoolError, TimeoutType};

    use super::PostgresOperationsError;

    #[test]
    fn pool_timeouts_are_retryable_but_shutdown_and_configuration_errors_are_not() {
        for timeout in [TimeoutType::Wait, TimeoutType::Create, TimeoutType::Recycle] {
            assert!(matches!(
                PostgresOperationsError::classify_create("db".into(), PoolError::Timeout(timeout)),
                PostgresOperationsError::UnableToCreateDatabase { .. }
            ));
        }
        for error in [PoolError::Closed, PoolError::NoRuntimeSpecified] {
            assert!(matches!(
                PostgresOperationsError::classify_drop("db".into(), error),
                PostgresOperationsError::NonTransientError { .. }
            ));
        }
    }

    #[test]
    fn sqlstate_retry_policy_preserves_transient_and_permanent_failures() {
        for code in ["08000", "08006", "57P01", "57P03", "53200", "53300", "55006", "55P03"] {
            assert!(PostgresOperationsError::is_transient_code(code), "{code}");
        }
        for code in ["42P04", "3D000", "53100", "42501", "22012", "XX000"] {
            assert!(!PostgresOperationsError::is_transient_code(code), "{code}");
        }
    }
}
