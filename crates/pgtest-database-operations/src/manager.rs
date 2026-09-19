use std::ops::Div;

use pgtest_utils::read_string::ReadString;
use sqlx::{
    Execute, Pool, Postgres, QueryBuilder,
    postgres::{PgConnectOptions, PgPoolOptions},
};
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
    create_pool: Pool<Postgres>,
    cleanup_pool: Pool<Postgres>,
}

const MIN_SERVER_VERSION_NUM: u8 = 13;

#[hotpath::measure_all]
impl PostgresManager {
    pub async fn start(postgres_config: PostgresConfig) -> Result<Self, PostgresClientError> {
        if postgres_config.pgtest_pg_cretion_pool_connection == 0 {
            return Err(PostgresClientError::InvalidPoolSize("PGTEST_CREATION_POOL_CONNECTION"));
        }
        if postgres_config.pgtest_pg_cleanup_pool_connection == 0 {
            return Err(PostgresClientError::InvalidPoolSize("PGTEST_CLEANUP_POOL_CONNECTION"));
        }

        let connection_options = PgConnectOptions::new()
            .host(&postgres_config.pgtest_pg_host)
            .port(postgres_config.pgtest_pg_port)
            .username(&postgres_config.pgtest_pg_user)
            .password("postgres")
            .database("postgres");

        let create_pool = PgPoolOptions::new()
            .max_connections(postgres_config.pgtest_pg_cretion_pool_connection)
            .connect_with(connection_options.clone())
            .await
            .map_err(|error| {
                tracing::error!(%error, "unable to connect the database creation pool");
                PostgresClientError::UnableToConnectToPostgres(format!(
                    "postgres://{}@{}:{}/postgres",
                    postgres_config.pgtest_pg_user,
                    postgres_config.pgtest_pg_host,
                    postgres_config.pgtest_pg_port
                ))
            })?;

        if let Err(error) =
            Self::database_exists(&create_pool, &postgres_config.pgtest_pg_database).await
        {
            create_pool.close().await;
            return Err(error);
        }

        let pg_version = match Self::is_valid_version(&create_pool).await {
            Err(error) => {
                create_pool.close().await;
                return Err(error);
            }
            Ok(pg_server_version) => pg_server_version,
        };

        let cleanup_pool = match PgPoolOptions::new()
            .max_connections(postgres_config.pgtest_pg_cleanup_pool_connection)
            .connect_with(connection_options)
            .await
        {
            Ok(pool) => pool,
            Err(error) => {
                tracing::error!(%error, "unable to connect the database cleanup pool");
                create_pool.close().await;
                return Err(PostgresClientError::UnableToConnectToPostgres(format!(
                    "postgres://{}@{}:{}/postgres",
                    postgres_config.pgtest_pg_user,
                    postgres_config.pgtest_pg_host,
                    postgres_config.pgtest_pg_port
                )));
            }
        };

        Ok(Self {
            version: pg_version,
            create_pool,
            cleanup_pool,
            template_database_name: PostgresDatabaseName::new(postgres_config.pgtest_pg_database),
            host: postgres_config.pgtest_pg_host,
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
        let mut query: QueryBuilder<Postgres> = sqlx::QueryBuilder::new(format!(
            "DROP DATABASE IF EXISTS {quoted_database_name} WITH (FORCE)"
        ));

        let result = async {
            let mut connection = self.acquire_drop_connection().await?;
            sqlx::query(query.build().sql()).execute(&mut *connection).await
        }
        .await;

        match result.map_err(|error| {
            tracing::warn!(database_name, error = ?error, "PostgreSQL DROP DATABASE failed");
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
                .fetch_all(&self.cleanup_pool)
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

        let result = async {
            let mut connection = self.acquire_create_connection().await?;
            sqlx::query(query.build().sql()).execute(&mut *connection).await
        }
        .await;

        result.map_err(|error| {
            PostgresOperationsError::classify_create(database_name.clone(), error)
        })?;

        Ok(ReadString::from(database_name))
    }

    // Separate acquisition timings from SQLx's query timings, which start after
    // a connection has been acquired. Keep create/drop separate to compare
    // waits.
    async fn acquire_create_connection(
        &self,
    ) -> Result<sqlx::pool::PoolConnection<Postgres>, sqlx::Error> {
        self.create_pool.acquire().await
    }

    async fn acquire_drop_connection(
        &self,
    ) -> Result<sqlx::pool::PoolConnection<Postgres>, sqlx::Error> {
        self.cleanup_pool.acquire().await
    }
}

#[cfg(any(test, feature = "test-support"))]
mod postgres_manager_test {
    use tokio::time::Instant;

    use super::{PostgresClientError, PostgresConfig, PostgresManager};
    use crate::testcontainer::pg_container_config;

    #[tokio::test]
    async fn zero_pool_limits_are_rejected_before_connecting() {
        for (config, variable) in [
            (
                PostgresConfig {
                    pgtest_pg_cretion_pool_connection: 0,
                    ..PostgresConfig::default()
                },
                "PGTEST_POOL_CONNECTION",
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
            pgtest_pg_cretion_pool_connection: 1,
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
        manager.create_pool.close().await;
        manager.cleanup_pool.close().await;
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
}
