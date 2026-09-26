use std::time::Duration;

use deadpool_postgres::{Client, Config, Pool, PoolConfig, PoolError, Runtime, Timeouts};
use futures_util::future::join_all;
use pgtest_utils::read_string::ReadString;
use tokio_postgres::NoTls;
use tokio_retry2::RetryError;

use crate::manager::{
    config::PostgresConfig,
    database_name::PostgresDatabaseName,
    errors::{PostgresClientError, PostgresOperationsError},
};

pub mod config;
pub mod database_name;
pub mod errors;

pub struct PostgresManager {
    pub version: u8,
    pub host: String,
    pub port: u16,
    pub template_database_name: PostgresDatabaseName,
    create_pool: Pool,
    cleanup_pool: Pool,
}

const MIN_SERVER_VERSION_NUM: u8 = 13;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
const CLEANUP_PIPELINE_DEPTH: usize = 32;

impl From<&PostgresConfig> for Config {
    fn from(value: &PostgresConfig) -> Self {
        let mut config = Config::new();
        config.host = Some(value.pgtest_pg_host.clone());
        config.port = Some(value.pgtest_pg_port);
        config.user = Some(value.pgtest_pg_user.clone());
        config.password = Some(String::from("postgres"));
        config.dbname = Some(String::from("postgres"));
        config
    }
}

#[hotpath::measure_all]
impl PostgresManager {
    pub async fn start(config: PostgresConfig) -> Result<Self, PostgresClientError> {
        if config.pgtest_pg_creation_pool_connection == 0 {
            return Err(PostgresClientError::InvalidPoolSize("PGTEST_CREATION_POOL_CONNECTION"));
        }
        if config.pgtest_pg_cleanup_pool_connection == 0 {
            return Err(PostgresClientError::InvalidPoolSize("PGTEST_CLEANUP_POOL_CONNECTION"));
        }

        let create_pool =
            Self::connect_pool(&config, config.pgtest_pg_creation_pool_connection, "creation")
                .await?;
        let validation = async {
            Self::database_exists(&create_pool, &config.pgtest_pg_database).await?;
            Self::is_valid_version(&create_pool).await
        }
        .await;
        let version = match validation {
            Ok(version) => version,
            Err(error) => {
                create_pool.close();
                return Err(error);
            }
        };
        let cleanup_pool =
            match Self::connect_pool(&config, config.pgtest_pg_cleanup_pool_connection, "cleanup")
                .await
            {
                Ok(pool) => pool,
                Err(error) => {
                    create_pool.close();
                    return Err(error);
                }
            };

        Ok(Self {
            version,
            create_pool,
            cleanup_pool,
            template_database_name: PostgresDatabaseName::new(config.pgtest_pg_database),
            host: config.pgtest_pg_host,
            port: config.pgtest_pg_port,
        })
    }

    async fn connect_pool(
        config: &PostgresConfig,
        max_connections: u32,
        purpose: &str,
    ) -> Result<Pool, PostgresClientError> {
        let connection_error = || {
            PostgresClientError::UnableToConnectToPostgres(format!(
                "postgres://{}@{}:{}/postgres",
                config.pgtest_pg_user, config.pgtest_pg_host, config.pgtest_pg_port,
            ))
        };
        let mut options = Config::from(config);
        let mut pool_config = PoolConfig::new(max_connections as usize);
        pool_config.timeouts = Timeouts {
            wait: Some(CONNECTION_TIMEOUT),
            create: Some(CONNECTION_TIMEOUT),
            recycle: Some(CONNECTION_TIMEOUT),
        };
        options.pool = Some(pool_config);
        let pool = options.create_pool(Some(Runtime::Tokio1), NoTls).map_err(|error| {
            tracing::error!(%error, purpose, "unable to configure PostgreSQL pool");
            connection_error()
        })?;
        // Deadpool builds lazily. Verify both pools during startup, not on
        // first DDL.
        if let Err(error) = pool.get().await {
            tracing::error!(%error, purpose, "unable to connect PostgreSQL pool");
            pool.close();
            return Err(connection_error());
        }
        Ok(pool)
    }

    async fn database_exists(pool: &Pool, database_name: &str) -> Result<(), PostgresClientError> {
        let client =
            pool.get().await.map_err(|_| PostgresClientError::UnableToFetchDatabaseList)?;
        let rows = client
            .query("SELECT datname FROM pg_database WHERE datname LIKE $1", &[&database_name])
            .await
            .map_err(|_| PostgresClientError::UnableToFetchDatabaseList)?;
        if rows.is_empty() {
            return Err(PostgresClientError::DatabaseDoesNotExist(database_name.to_owned()));
        }
        Ok(())
    }

    async fn is_valid_version(pool: &Pool) -> Result<u8, PostgresClientError> {
        let client =
            pool.get().await.map_err(|_| PostgresClientError::UnableToFetchPostgresVersion)?;
        let row = client
            .query_one("SELECT current_setting('server_version_num')::int8", &[])
            .await
            .map_err(|_| PostgresClientError::UnableToFetchPostgresVersion)?;
        let number: i64 =
            row.try_get(0).map_err(|_| PostgresClientError::UnableToFetchPostgresVersion)?;
        let version = u8::try_from(number / 10000)
            .map_err(|_| PostgresClientError::UnexpectedServerVersionFormatFetched(number))?;
        if version < MIN_SERVER_VERSION_NUM {
            return Err(PostgresClientError::UnsupportedVersion(version));
        }
        Ok(version)
    }

    pub async fn drop_ddl_database(
        &self,
        database_name: &str,
    ) -> Result<(), RetryError<PostgresOperationsError>> {
        let client = self
            .acquire_drop_connection()
            .await
            .map_err(|error| Self::drop_error(database_name, error))?;
        Self::drop_on_connection(&client, database_name).await
    }

    fn drop_error(database_name: &str, error: PoolError) -> RetryError<PostgresOperationsError> {
        tracing::warn!(database_name, %error, "PostgreSQL DROP DATABASE failed");
        let error = PostgresOperationsError::classify_drop(database_name.to_owned(), error);
        match error {
            error @ PostgresOperationsError::NonTransientError { .. } => {
                RetryError::Permanent(error)
            }
            error => RetryError::Transient { err: error, retry_after: None },
        }
    }

    async fn drop_on_connection(
        client: &tokio_postgres::Client,
        database_name: &str,
    ) -> Result<(), RetryError<PostgresOperationsError>> {
        let quoted = PostgresDatabaseName::quote_ident(database_name);
        let query = format!("DROP DATABASE IF EXISTS {quoted} WITH (FORCE)");
        Self::execute_ddl(client, &query)
            .await
            .map_err(|error| Self::drop_error(database_name, error.into()))?;
        Ok(())
    }

    pub async fn drop_ddl_templates_like(&self) -> Result<(), RetryError<PostgresOperationsError>> {
        let client = self.acquire_drop_connection().await.map_err(|error| {
            RetryError::Permanent(PostgresOperationsError::UnableToListDatabases(error))
        })?;
        let rows = client
            .query(
                "SELECT datname FROM pg_database WHERE datname LIKE $1",
                &[&format!("{}_%", self.template_database_name.template_name())],
            )
            .await
            .map_err(|error| {
                RetryError::Permanent(PostgresOperationsError::UnableToListDatabases(error.into()))
            })?;
        let names: Vec<String> =
            rows.iter().map(|row| row.try_get(0)).collect::<Result<_, _>>().map_err(|error| {
                RetryError::Permanent(PostgresOperationsError::UnableToListDatabases(error.into()))
            })?;

        for batch in names.chunks(CLEANUP_PIPELINE_DEPTH) {
            let results =
                join_all(batch.iter().map(|name| Self::drop_on_connection(&client, name))).await;
            for result in results {
                result?;
            }
        }
        Ok(())
    }

    pub async fn create_ddl_database(
        &self,
    ) -> Result<ReadString, RetryError<PostgresOperationsError>> {
        let database_name = self.template_database_name.generate_database_name();
        let quoted_database = PostgresDatabaseName::quote_ident(&database_name);
        let quoted_template =
            PostgresDatabaseName::quote_ident(self.template_database_name.template_name());
        let mut query = format!("CREATE DATABASE {quoted_database} TEMPLATE {quoted_template}");
        if self.version >= 15 {
            query.push_str(" STRATEGY=FILE_COPY");
        }
        let result: Result<(), PoolError> = async {
            let client = self.acquire_create_connection().await?;
            Self::execute_ddl(&client, &query).await?;
            Ok(())
        }
        .await;
        result.map_err(|error| {
            let error = PostgresOperationsError::classify_create(database_name.clone(), error);
            match error {
                error @ PostgresOperationsError::NonTransientError { .. } => {
                    RetryError::Permanent(error)
                }
                error => RetryError::Transient { err: error, retry_after: None },
            }
        })?;
        Ok(ReadString::from(database_name))
    }

    async fn execute_ddl(
        client: &tokio_postgres::Client,
        query: &str,
    ) -> Result<(), tokio_postgres::Error> {
        client.execute_typed(query, &[]).await?;
        Ok(())
    }

    async fn acquire_create_connection(&self) -> Result<Client, PoolError> {
        self.create_pool.get().await
    }

    async fn acquire_drop_connection(&self) -> Result<Client, PoolError> {
        self.cleanup_pool.get().await
    }
}

#[cfg(test)]
mod postgres_manager_test {
    use tokio::time::Instant;

    use super::{PostgresClientError, PostgresConfig, PostgresManager};
    use crate::testcontainer::pg_container_config;

    #[tokio::test]
    async fn zero_pool_limits_are_rejected_before_connecting() {
        for (config, variable) in [
            (
                PostgresConfig {
                    pgtest_pg_creation_pool_connection: 0,
                    ..PostgresConfig::default()
                },
                "PGTEST_CREATION_POOL_CONNECTION",
            ),
            (
                PostgresConfig {
                    pgtest_pg_cleanup_pool_connection: 0,
                    ..PostgresConfig::default()
                },
                "PGTEST_CLEANUP_POOL_CONNECTION",
            ),
        ] {
            assert!(matches!(
                PostgresManager::start(config).await,
                Err(PostgresClientError::InvalidPoolSize(actual)) if actual == variable
            ));
        }
    }

    #[tokio::test]
    async fn cleanup_and_creation_have_independent_connection_capacity() {
        let config = PostgresConfig {
            pgtest_pg_creation_pool_connection: 1,
            pgtest_pg_cleanup_pool_connection: 1,
            ..pg_container_config().await
        };
        let manager = PostgresManager::start(config).await.unwrap();

        let cleanup_connection = manager.acquire_drop_connection().await.unwrap();
        let creation_connection = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            manager.acquire_create_connection(),
        )
        .await
        .expect("a full cleanup pool must not block creation")
        .unwrap();

        drop(cleanup_connection);
        let cleanup_connection = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            manager.acquire_drop_connection(),
        )
        .await
        .expect("a full creation pool must not block cleanup")
        .unwrap();

        drop(creation_connection);
        drop(cleanup_connection);
        manager.create_pool.close();
        manager.cleanup_pool.close();
    }

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

        // A cleanup retry may follow a lost response to a successful DROP.
        manager.drop_ddl_database(&database_name).await.unwrap();
    }

    #[tokio::test]
    async fn startup_connects_eagerly_and_reports_unreachable_postgres() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let config = PostgresConfig {
            pgtest_pg_host: "127.0.0.1".into(),
            pgtest_pg_port: port,
            ..PostgresConfig::default()
        };
        assert!(matches!(
            PostgresManager::start(config).await,
            Err(PostgresClientError::UnableToConnectToPostgres(_))
        ));
    }

    #[tokio::test]
    async fn pipelined_cleanup_works_with_one_connection_and_quoted_names() {
        let config = PostgresConfig {
            pgtest_pg_creation_pool_connection: 1,
            pgtest_pg_cleanup_pool_connection: 1,
            ..pg_container_config().await
        };
        let manager = PostgresManager::start(config).await.unwrap();
        let client = manager.acquire_create_connection().await.unwrap();
        let prefix = manager.template_database_name.template_name();
        // Cross the batch boundary and include names that require SQL quoting.
        let names: Vec<_> = (0..=super::CLEANUP_PIPELINE_DEPTH)
            .map(|index| format!("{prefix}_Mixed \"{index}"))
            .collect();
        let template = super::PostgresDatabaseName::quote_ident(prefix);
        let queries: Vec<_> = names
            .iter()
            .map(|name| {
                format!(
                    "CREATE DATABASE {} TEMPLATE {template}",
                    super::PostgresDatabaseName::quote_ident(name),
                )
            })
            .collect();
        for result in futures_util::future::join_all(
            queries.iter().map(|sql| PostgresManager::execute_ddl(&client, sql)),
        )
        .await
        {
            result.expect("pipelined CREATE must run outside a transaction block");
        }
        drop(client);

        tokio::time::timeout(std::time::Duration::from_secs(30), manager.drop_ddl_templates_like())
            .await
            .expect("cleanup must not wait for a second pool connection")
            .expect("all pipelined drops must execute");
        let client = manager.acquire_drop_connection().await.unwrap();
        let row = client
            .query_one("SELECT count(*) FROM pg_database WHERE datname::text = ANY($1)", &[&names])
            .await
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 0);
        let template_exists = client
            .query_one("SELECT EXISTS(SELECT FROM pg_database WHERE datname = $1)", &[&prefix])
            .await
            .unwrap();
        assert!(template_exists.get::<_, bool>(0));
    }

    #[tokio::test]
    async fn pipeline_failure_does_not_skip_later_commands_or_poison_connection() {
        let manager = PostgresManager::start(pg_container_config().await).await.unwrap();
        let database = manager.create_ddl_database().await.unwrap();
        let client = manager.acquire_drop_connection().await.unwrap();
        let (failure, success) = tokio::join!(
            PostgresManager::execute_ddl(&client, "SELECT 1 / 0"),
            PostgresManager::drop_on_connection(&client, &database),
        );
        assert_eq!(failure.unwrap_err().code().unwrap().code(), "22012");
        success.expect("a separate Sync must allow the next DROP to succeed");
        client.simple_query("SELECT 1").await.expect("connection must remain usable");
    }

    #[tokio::test]
    async fn driver_errors_and_closed_pools_keep_retry_classification() {
        use tokio_retry2::RetryError;

        use super::errors::PostgresOperationsError;

        let manager = PostgresManager::start(pg_container_config().await).await.unwrap();
        let client = manager.acquire_create_connection().await.unwrap();
        for (code, transient) in [("55006", true), ("42501", false), ("42P04", false)] {
            let error = client
                .batch_execute(&format!(
                    "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '{code}', MESSAGE = 'test'; END \
                     $$"
                ))
                .await
                .unwrap_err();
            let classified = PostgresOperationsError::classify_create("db".into(), error.into());
            assert_eq!(
                matches!(classified, PostgresOperationsError::UnableToCreateDatabase { .. }),
                transient
            );
        }
        drop(client);
        manager.create_pool.close();
        manager.cleanup_pool.close();
        assert!(matches!(
            manager.create_ddl_database().await,
            Err(RetryError::Permanent(PostgresOperationsError::NonTransientError { .. }))
        ));
        assert!(matches!(
            manager.drop_ddl_database("db").await,
            Err(RetryError::Permanent(PostgresOperationsError::NonTransientError { .. }))
        ));
        assert!(manager.drop_ddl_templates_like().await.is_err());
    }
}
