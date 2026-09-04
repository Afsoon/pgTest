use std::ops::Div;
#[cfg(all(test, feature = "stable_ids"))]
use std::sync::atomic::AtomicUsize;

use envconfig::Envconfig;
#[cfg(any(test, feature = "test-support"))]
use pgtest_container::POSTGRES_CONTAINER;
#[cfg(not(all(test, feature = "stable_ids")))]
use rand::{RngExt, rng};
use sqlx::{
    Execute, Pool, Postgres, QueryBuilder,
    postgres::{PgConnectOptions, PgPoolOptions},
};
#[cfg(any(test, feature = "test-support"))]
use testcontainers::{Container, core::ContainerPort};
use thiserror::Error;
use tokio_retry2::{
    Retry, RetryError,
    strategy::{ExponentialFactorBackoff, jitter},
};

use crate::{
    utils::ReadString,
    worker_engine::{errors::PostgresDDLClientError, traits::PostgresClient},
};

#[derive(Envconfig, Debug)]
pub struct PostgresConfig {
    #[envconfig(from = "PGTEST_PG_HOST", default = "127.0.0.1")]
    pub pgtest_pg_host: String,
    #[envconfig(from = "PGTEST_PG_PORT", default = "5432")]
    pub pgtest_pg_port: u16,
    #[envconfig(from = "PGTEST_PG_USER", default = "postgres")]
    pub pgtest_pg_user: String,
    #[envconfig(from = "PGTEST_PG_DATABASE", default = "pgtest")]
    pub pgtest_pg_database: String,
    #[envconfig(from = "PGTEST_POOL_CONNECTION", default = "5")]
    pub pgtest_pg_pool_connection: u32,
}

const MIN_SERVER_VERSION_NUM: u8 = 13;

pub struct PostgresManager {
    version: u8,
    pg: Pool<Postgres>,
    pub port: u16,
    pub template_database_name: PostgresDatabaseName,
}

#[derive(Error, Debug)]
pub enum PostgresClientError {
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
    fn classify_create(database_name: String, source: sqlx::Error) -> Self {
        if is_transient(&source) {
            PostgresOperationsError::UnableToCreateDatabase { database_name, source }
        } else {
            PostgresOperationsError::NonTransientError { database_name, source }
        }
    }

    fn classify_drop(database_name: String, source: sqlx::Error) -> Self {
        if is_transient(&source) {
            PostgresOperationsError::UnableToDropDatabase { database_name, source }
        } else {
            PostgresOperationsError::NonTransientError { database_name, source }
        }
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

#[cfg(not(all(test, feature = "stable_ids")))]
const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
abcdefghijklmnopqrstuvwxyz";

/// Length of the random identifier appended to generated database names.
const GENERATED_ID_LEN: usize = 32;

pub struct PostgresDatabaseName {
    name: String,
    prefix_template: String,
    #[cfg(all(test, feature = "stable_ids"))]
    seq: AtomicUsize,
}

impl PostgresDatabaseName {
    pub fn new(database_name: String) -> Self {
        let prefix_template = PostgresDatabaseName::truncate(&database_name);

        Self {
            name: database_name,
            prefix_template,
            #[cfg(all(test, feature = "stable_ids"))]
            seq: AtomicUsize::new(0),
        }
    }

    #[cfg(not(all(test, feature = "stable_ids")))]
    fn generate_parallel_test_id() -> String {
        let mut random_gen = rng();
        let template_name_len: usize = 4;

        let mut template_identifier = String::with_capacity(template_name_len);

        for _ in 0..template_name_len {
            let idx = random_gen.random_range(0..CHARSET.len());
            template_identifier.push(CHARSET[idx] as char);
        }

        template_identifier
    }

    #[cfg(not(all(test, feature = "stable_ids")))]
    fn generate_id(&self) -> String {
        let mut random_gen = rng();

        let mut template_identifier = String::with_capacity(GENERATED_ID_LEN);

        for _ in 0..GENERATED_ID_LEN {
            let idx = random_gen.random_range(0..CHARSET.len());
            template_identifier.push(CHARSET[idx] as char);
        }

        template_identifier
    }

    #[cfg(all(test, feature = "stable_ids"))]
    fn generate_id(&self) -> String {
        (self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1).to_string()
    }

    #[cfg(all(test, feature = "stable_ids"))]
    fn truncate(database_name: &str) -> String {
        // Postgres identifiers are limited to 63 bytes; generated names are
        // `{prefix}_{id}`, so the prefix may use at most 63 - id - 1 bytes.
        const MAX_PREFIX_LEN: usize = 63 - GENERATED_ID_LEN - 1;

        if database_name.len() <= MAX_PREFIX_LEN {
            return database_name.to_string();
        }

        let mut cut = MAX_PREFIX_LEN;
        while !database_name.is_char_boundary(cut) {
            cut -= 1;
        }

        database_name[..cut].to_string()
    }

    #[cfg(not(all(test, feature = "stable_ids")))]
    fn truncate(database_name: &str) -> String {
        // Postgres identifiers are limited to 63 bytes; generated names are
        // `{prefix}_{id}`, so the prefix may use at most 63 - id - 1 bytes.
        const MAX_PREFIX_LEN: usize = 63 - GENERATED_ID_LEN - 1 - 4 - 1;

        if database_name.len() <= MAX_PREFIX_LEN {
            return database_name.to_string();
        }

        let mut cut = MAX_PREFIX_LEN;
        while !database_name.is_char_boundary(cut) {
            cut -= 1;
        }

        format!(
            "{}_{}",
            database_name[..cut].to_string(),
            PostgresDatabaseName::generate_parallel_test_id()
        )
    }

    pub fn generate_database_name(&self) -> String {
        let identifier = self.generate_id();

        format!("{}_{}", self.prefix_template, identifier)
    }

    pub fn template_name<'a>(&'a self) -> &'a str {
        &self.name
    }

    fn quote_ident(ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }
}

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
        let database_name = self.template_database_name.generate_database_name();
        let quoted_database_name = PostgresDatabaseName::quote_ident(&database_name);
        let quoted_template_database_name =
            PostgresDatabaseName::quote_ident(&self.template_database_name.template_name());

        let mut query: QueryBuilder<Postgres> = sqlx::QueryBuilder::new("CREATE DATABASE ");
        query.push(format!("{quoted_database_name} TEMPLATE {quoted_template_database_name}"));

        if self.version >= 15 {
            query.push(format!(" STRATEGY=FILE_COPY"));
        };

        let retry_strategy = ExponentialFactorBackoff::from_millis(250, 1.0).map(jitter).take(3);
        match Retry::spawn(retry_strategy, || self.create_ddl_database()).await {
            Ok(database_name) => Ok(ReadString::from(database_name)),
            Err(PostgresOperationsError::NonTransientError { .. }) => {
                let message_error = format!(
                    "create the database {database_name} from the template \
                     ${quoted_template_database_name}"
                );
                Err(PostgresDDLClientError::NonRecoverableError(message_error))
            }
            Err(_) => {
                let message_error = format!(
                    "create the database {database_name} from the template \
                     ${quoted_template_database_name}"
                );
                Err(PostgresDDLClientError::OperationNotExecutedAfterCertainRetries {
                    operation: message_error,
                    retries: 3,
                })
            }
        }
    }
}

impl PostgresManager {
    pub async fn start(
        postgres_config: PostgresConfig,
    ) -> Result<PostgresManager, PostgresClientError> {
        let connection_options = PgConnectOptions::new()
            .host(&postgres_config.pgtest_pg_host)
            .port(postgres_config.pgtest_pg_port)
            .username(&postgres_config.pgtest_pg_user)
            .password("postgres")
            .database("postgres");

        let Ok(pool) = PgPoolOptions::new()
            .max_connections(postgres_config.pgtest_pg_pool_connection)
            .connect_with(connection_options)
            .await
        else {
            return Err(PostgresClientError::UnableToConnectToPostgres(format!(
                "postgres://{}@{}:{}/postgres",
                postgres_config.pgtest_pg_user,
                postgres_config.pgtest_pg_host,
                postgres_config.pgtest_pg_port
            )));
        };

        PostgresManager::database_exists(&pool, &postgres_config.pgtest_pg_database).await?;

        let pg_version = match PostgresManager::is_valid_version(&pool).await {
            Err(error) => return Err(error),
            Ok(pg_server_version) => pg_server_version,
        };

        Ok(PostgresManager {
            version: pg_version,
            pg: pool,
            template_database_name: PostgresDatabaseName::new(postgres_config.pgtest_pg_database),
            port: postgres_config.pgtest_pg_port,
        })
    }

    async fn database_exists(
        pool: &Pool<Postgres>,
        database_name: &str,
    ) -> Result<(), PostgresClientError> {
        let database_exists: Result<Vec<(String,)>, sqlx::Error> =
            sqlx::query_as("SELECT datname from pg_database WHERE datname LIKE $1")
                .bind(database_name)
                .fetch_all(pool)
                .await;

        match database_exists {
            Err(_) => return Err(PostgresClientError::UnableToFetchDatabaseList),
            Ok(result) if result.len() == 0 => {
                return Err(PostgresClientError::DatabaseDoesNotExist(database_name.to_string()));
            }
            Ok(_) => return Ok(()),
        }
    }

    async fn is_valid_version(pool: &Pool<Postgres>) -> Result<u8, PostgresClientError> {
        let server_version_result: Result<(i64,), sqlx::Error> =
            sqlx::query_as("SELECT current_setting('server_version_num')::int8")
                .fetch_one(pool)
                .await;

        let server_version = match server_version_result {
            Ok((server_version_value,)) => server_version_value,
            Err(_) => return Err(PostgresClientError::UnableToFetchPostgresVersion),
        };

        let Ok(server_version): Result<u8, _> = server_version.div(10000).try_into() else {
            return Err(PostgresClientError::UnexpectedServerVersionFormatFetched(server_version));
        };

        if MIN_SERVER_VERSION_NUM > server_version {
            return Err(PostgresClientError::UnsupportedVersion(server_version));
        }

        return Ok(server_version);
    }

    pub async fn drop_ddl_database(
        &self,
        database_name: &str,
    ) -> Result<(), RetryError<PostgresOperationsError>> {
        let quoted_database_name = PostgresDatabaseName::quote_ident(database_name);
        let mut query: QueryBuilder<Postgres> =
            sqlx::QueryBuilder::new(format!("DROP DATABASE {quoted_database_name} WITH (FORCE)"));

        match sqlx::query(query.build().sql()).execute(&self.pg).await.map_err(|error| {
            PostgresOperationsError::classify_drop(database_name.to_string(), error)
        }) {
            Ok(_) => Ok(()),
            Err(error @ PostgresOperationsError::NonTransientError { .. }) => {
                RetryError::to_permanent(error)
            }
            Err(error) => RetryError::to_transient(error),
        }
    }

    pub async fn drop_ddl_templates_like(&self) -> Result<(), RetryError<PostgresOperationsError>> {
        let templates_result: Result<Vec<(String,)>, sqlx::Error> =
            sqlx::query_as("SELECT datname from pg_database WHERE datname LIKE $1")
                .bind(format!("{}_%", self.template_database_name.template_name()))
                .fetch_all(&self.pg)
                .await;

        let templates = match templates_result {
            Ok(templates) => templates,
            Err(error) => {
                return Err(RetryError::Permanent(PostgresOperationsError::UnableToListDatabases(
                    error,
                )));
            }
        };

        for (template,) in templates {
            self.drop_ddl_database(&template).await?;
        }

        Ok(())
    }

    pub async fn create_ddl_database(
        &self,
    ) -> Result<ReadString, RetryError<PostgresOperationsError>> {
        let database_name = self.template_database_name.generate_database_name();
        let quoted_database_name = PostgresDatabaseName::quote_ident(&database_name);
        let quoted_template_database_name =
            PostgresDatabaseName::quote_ident(&self.template_database_name.template_name());

        let mut query: QueryBuilder<Postgres> = sqlx::QueryBuilder::new("CREATE DATABASE ");
        query.push(format!("{quoted_database_name} TEMPLATE {quoted_template_database_name}"));

        if self.version >= 15 {
            query.push(format!(" STRATEGY=FILE_COPY"));
        };

        sqlx::query(query.build().sql()).execute(&self.pg).await.map_err(|error| {
            PostgresOperationsError::classify_create(database_name.clone(), error)
        })?;

        Ok(ReadString::from(database_name))
    }
}

// TODO use testcontainers to test this.
// TODO better test naming
#[cfg(test)]
mod postgres_manager_test {
    use tokio::time::Instant;

    use crate::postgres_manager::{PostgresDatabaseName, PostgresManager, pg_container_config};

    #[tokio::test]
    async fn start_ok() {
        let manager = PostgresManager::start(pg_container_config().await).await.unwrap();

        assert_eq!(manager.version, 18);

        let now_create = Instant::now();
        let database_name = manager.create_ddl_database().await.unwrap();
        println!("Creating time {:.2?}", now_create.elapsed());

        let now_drop = Instant::now();
        manager.drop_ddl_database(&database_name).await.unwrap();
        println!("Drop time {:.2?}", now_drop.elapsed());
    }

    #[test]
    #[ignore = "Until we have a new feature condition for test"]
    fn truncate_never_underflows_and_fits_identifier() {
        let long_name = "a".repeat(40);
        let truncated = PostgresDatabaseName::truncate(&long_name);
        assert_eq!(truncated.len(), 30); // 63 - 32-char id - 1 underscore

        assert_eq!(PostgresDatabaseName::truncate("pgtest"), "pgtest");

        let boundary_name = "b".repeat(30);
        assert_eq!(PostgresDatabaseName::truncate(&boundary_name), boundary_name);
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            pgtest_pg_database: String::from("pgtest"),
            pgtest_pg_port: 5432,
            pgtest_pg_user: String::from("postgres"),
            pgtest_pg_host: String::from("localhost"),
            pgtest_pg_pool_connection: 5,
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl<'a> From<&'a Container<pgtest_container::Postgres>> for PostgresConfig {
    fn from(value: &'a Container<pgtest_container::Postgres>) -> Self {
        Self {
            pgtest_pg_host: value.get_host().unwrap().to_string(),
            pgtest_pg_port: value.get_host_port_ipv4(ContainerPort::Tcp(5432)).unwrap(),
            ..PostgresConfig::default()
        }
    }
}

// https://github.com/tokio-rs/tokio/discussions/3857
#[cfg(any(test, feature = "test-support"))]
pub async fn pg_container_config() -> PostgresConfig {
    tokio::task::spawn_blocking(|| PostgresConfig::from(&*POSTGRES_CONTAINER)).await.unwrap()
}
