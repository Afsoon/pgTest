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
        source: sqlx::Error,
    },
    #[error("transient failure creating database {database_name}: {source}")]
    UnableToCreateDatabase {
        database_name: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("transient failure dropping database {database_name}: {source}")]
    UnableToDropDatabase {
        database_name: String,
        #[source]
        source: sqlx::Error,
    },
    #[error("unable to list databases: {0}")]
    UnableToListDatabases(#[source] sqlx::Error),
}

impl PostgresOperationsError {
    pub fn classify_create(database_name: String, source: sqlx::Error) -> Self {
        if PostgresOperationsError::is_transient(&source) {
            PostgresOperationsError::UnableToCreateDatabase { database_name, source }
        } else {
            PostgresOperationsError::NonTransientError { database_name, source }
        }
    }

    pub fn classify_drop(database_name: String, source: sqlx::Error) -> Self {
        if PostgresOperationsError::is_transient(&source) {
            PostgresOperationsError::UnableToDropDatabase { database_name, source }
        } else {
            PostgresOperationsError::NonTransientError { database_name, source }
        }
    }

    /// Whether a failed postgres operation is worth retrying.
    ///
    /// Transient: connectivity blips (class 08), server restarting (57P*),
    /// resource pressure that clears (53200 out of memory, 53300 too many
    /// connections) and busy objects (55006 template in use, 55P03 lock not
    /// available). Everything else is non-transient by default: 42P04 duplicate
    /// database, 3D000 missing template, 53100 disk full, privilege errors,
    /// unknown codes, decode/config errors, a closed pool.
    fn is_transient(err: &sqlx::Error) -> bool {
        match err {
            sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut => true,
            sqlx::Error::Database(db) => match db.code().as_deref() {
                Some(code) => {
                    code.starts_with("08")
                        || code.starts_with("57P")
                        || code == "53200"
                        || code == "53300"
                        || code == "55006"
                        || code == "55P03"
                }
                None => false,
            },
            _ => false,
        }
    }
}
