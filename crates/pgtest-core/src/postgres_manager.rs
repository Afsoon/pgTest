use std::ops::Div;

use envconfig::Envconfig;
use rand::{RngExt, rng};
use sqlx::{Execute, Pool, Postgres, QueryBuilder, postgres::PgPoolOptions};
use thiserror::Error;

#[derive(Envconfig, Debug)]
pub struct PostgresConfig {
    #[envconfig(from = "PGTEST_PG_HOST", default = "127.0.0.1")]
    pub pgtest_pg_host: String,
    #[envconfig(from = "PGTEST_PG_PORT", default = "5432")]
    pub pgtest_pg_port: u16,
    #[envconfig(from = "PGTEST_PG_DATABASE", default = "pgtest")]
    pub pgtest_pg_database: String,
    // TODO: admin connections size env
}

const MIN_SERVER_VERSION_NUM: u8 = 13;

pub struct PostgresManager {
    version: u8,
    pg: Pool<Postgres>,
    pub template_database_name: String,
}

#[derive(Error, Debug)]
pub enum PostgresClientError {
    #[error("Unable to connect to postgrest at {0}")]
    UnableToConnectToPostgres(String),
    #[error("Unable to fetch postgres version")]
    UnableToFetchPostgresVersion,
    #[error("Unable to fetch available databases")]
    UnableToFetchDatabaseList,
    #[error("Database {0} doesn't exist")]
    DatabaseDoesNotExists(String),
    #[error("invalid postgresql version {0}. Expected PosgreSQL version 13 or higher")]
    UnsoportedVersion(u8),
    #[error(
        "Unexpected server version format fetched from database. Fetched {0}, expected an unsigned integer like 130_000"
    )]
    UnexpectedServerVersionFormatFetched(i64),
}

const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
abcdefghijklmnopqrstuvwxyz";

impl PostgresManager {
    pub async fn start(
        postgres_config: &PostgresConfig,
    ) -> Result<PostgresManager, PostgresClientError> {
        let connection_string =
            format!("postgres://trust:trust@{}/postgres", postgres_config.pgtest_pg_host);

        let Ok(pool) = PgPoolOptions::new().max_connections(5).connect(&connection_string).await
        else {
            return Err(PostgresClientError::UnableToConnectToPostgres(connection_string));
        };

        PostgresManager::database_exists(&pool, &postgres_config.pgtest_pg_database).await?;

        let pg_version = match PostgresManager::is_valid_version(&pool).await {
            Err(error) => return Err(error),
            Ok(pg_server_version) => pg_server_version,
        };

        Ok(PostgresManager {
            version: pg_version,
            pg: pool,
            template_database_name: postgres_config.pgtest_pg_database.clone(),
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
                return Err(PostgresClientError::DatabaseDoesNotExists(database_name.to_string()));
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
            return Err(PostgresClientError::UnsoportedVersion(server_version));
        }

        return Ok(server_version);
    }

    fn generate_database_name(&self) -> String {
        let mut random_gen = rng();

        let mut template_identifier = String::with_capacity(32);

        for _ in 0..32 {
            let idx = random_gen.random_range(0..CHARSET.len());
            template_identifier.push(CHARSET[idx] as char);
        }

        format!("{}_{}", self.template_database_name, template_identifier)
    }

    pub async fn drop_database(&self, database_name: &str) -> Result<(), PostgresClientError> {
        let quoted_database_name = quote_ident(database_name);
        let mut query: QueryBuilder<Postgres> =
            sqlx::QueryBuilder::new(format!("DROP DATABASE {quoted_database_name} WITH (FORCE)"));

        let _ = sqlx::query(query.build().sql()).bind(database_name).fetch_one(&self.pg).await;

        Ok(())
    }

    pub async fn drop_templates_like(&self) -> Result<(), PostgresClientError> {
        let templates_result: Result<Vec<(String,)>, sqlx::Error> =
            sqlx::query_as("SELECT datname from pg_database WHERE datname LIKE $1")
                .bind(format!("{}_%", self.template_database_name))
                .fetch_all(&self.pg)
                .await;

        let Ok(templates) = templates_result else {
            return Err(PostgresClientError::UnableToFetchDatabaseList); // TODO specific error;
        };

        for (template,) in templates {
            self.drop_database(&template).await?;
        }

        Ok(())
    }

    pub async fn create_database(&self) -> Result<String, PostgresClientError> {
        let database_name = self.generate_database_name();
        let quoted_database_name = quote_ident(&database_name);
        let quoted_template_database_name = quote_ident(&self.template_database_name);

        let mut query: QueryBuilder<Postgres> = sqlx::QueryBuilder::new("CREATE DATABASE ");
        query.push(format!("{quoted_database_name} TEMPLATE {quoted_template_database_name}"));

        if self.version >= 15 {
            query.push(format!(" STRATEGY=FILE_COPY"));
        };

        let _ = sqlx::query(query.build().sql()).fetch_one(&self.pg).await;

        Ok(database_name.to_string())
    }
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

// TODO use testcontainers to test this.
// TODO better test naming
#[cfg(test)]
mod postgres_manager_test {
    use tokio::time::Instant;

    use crate::postgres_manager::{PostgresConfig, PostgresManager};

    #[tokio::test]
    #[ignore]
    async fn start_ok() {
        let config = PostgresConfig {
            pgtest_pg_database: String::from("pgtest"),
            pgtest_pg_port: 5432,
            pgtest_pg_host: String::from("localhost"),
        };

        let manager = PostgresManager::start(&config).await.unwrap();

        assert_eq!(manager.version, 18);

        let now_create = Instant::now();
        let database_name = manager.create_database().await.unwrap();
        println!("Creating time {:.2?}", now_create.elapsed());

        let now_drop = Instant::now();
        manager.drop_database(&database_name).await.unwrap();
        println!("Drop time {:.2?}", now_drop.elapsed());
    }
}
