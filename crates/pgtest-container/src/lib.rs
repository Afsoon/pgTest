// 99% of this files is the same as-is in https://github.com/testcontainers/testcontainers-rs-modules-community/blob/main/src/postgres/mod.rs.
// I have only adapted for my needs, the config deactivate all PG safeguards for
// data safety, non essential work, and persist our data in memory.
use std::{borrow::Cow, collections::HashMap, sync::LazyLock};

use testcontainers::{
    Container, CopyDataSource, CopyToContainer, Image, ImageExt, core::WaitFor, runners::SyncRunner,
};

const NAME: &str = "postgres";
const TAG: &str = "18-alpine";

// Const donsn't work to keep a shared state between threads
pub static POSTGRES_CONTAINER: LazyLock<Container<Postgres>> = LazyLock::new(|| {
    let postgres = Postgres::default()
        .with_host_config_modifier(|config| {
            config.tmpfs.get_or_insert_default().insert(
                Postgres::mount_data_dir_post_pg_18(),
                "rw,noexec,nosuid,size=3g".to_owned(),
            );
        })
        .start()
        .unwrap();

    postgres
});

/// Module to work with [`Postgres`] inside of tests.
///
/// Starts an instance of Postgres.
/// This module is based on the official [`Postgres docker image`].
///
/// Default db name, user and password is `postgres`.
///
/// # Example
/// ```
/// use testcontainers_modules::{postgres, testcontainers::runners::SyncRunner};
///
/// let postgres_instance = postgres::Postgres::default().start().unwrap();
///
/// let connection_string = format!(
///     "postgres://postgres:postgres@{}:{}/postgres",
///     postgres_instance.get_host().unwrap(),
///     postgres_instance.get_host_port_ipv4(5432).unwrap()
/// );
/// ```
///
/// [`Postgres`]: https://www.postgresql.org/
/// [`Postgres docker image`]: https://hub.docker.com/_/postgres
#[derive(Debug, Clone)]
pub struct Postgres {
    env_vars: HashMap<String, String>,
    copy_to_sources: Vec<CopyToContainer>,
}

impl Postgres {
    /// Sets the db name for the Postgres instance.
    pub fn with_db_name(mut self, db_name: &str) -> Self {
        self.env_vars.insert("POSTGRES_DB".to_owned(), db_name.to_owned());
        self
    }

    /// Sets the user for the Postgres instance.
    pub fn with_user(mut self, user: &str) -> Self {
        self.env_vars.insert("POSTGRES_USER".to_owned(), user.to_owned());
        self
    }

    /// Sets the password for the Postgres instance.
    pub fn with_password(mut self, password: &str) -> Self {
        self.env_vars.insert("POSTGRES_PASSWORD".to_owned(), password.to_owned());
        self
    }

    pub fn mount_data_dir_post_pg_18() -> String {
        "/var/lib/postgresql".to_owned()
    }

    pub fn mount_data_dir_pre_pg_18() -> String {
        "/var/lib/postgresql/data".to_owned()
    }

    /// Registers sql to be executed automatically when the container starts.
    /// Can be called multiple times to add (not override) scripts.
    ///
    /// # Example
    ///
    /// ```
    /// # use testcontainers_modules::postgres::Postgres;
    /// let postgres_image = Postgres::default()
    ///     .with_init_sql("CREATE EXTENSION IF NOT EXISTS hstore;".to_string().into_bytes());
    /// ```
    ///
    /// ```rust,ignore
    /// # use testcontainers_modules::postgres::Postgres;
    /// let postgres_image = Postgres::default()
    ///                                .with_init_sql(include_str!("path_to_init.sql").to_string().into_bytes());
    /// ```
    pub fn with_init_sql(mut self, init_sql: impl Into<CopyDataSource>) -> Self {
        let target =
            format!("/docker-entrypoint-initdb.d/init_{i}.sql", i = self.copy_to_sources.len());
        self.copy_to_sources.push(CopyToContainer::new(init_sql.into(), target));
        self
    }
}

impl Default for Postgres {
    fn default() -> Self {
        let mut env_vars = HashMap::new();
        env_vars.insert("POSTGRES_DB".to_owned(), "pgtest".to_owned());
        env_vars.insert("POSTGRES_USER".to_owned(), "postgres".to_owned());
        env_vars.insert("POSTGRES_PASSWORD".to_owned(), "postgres".to_owned());
        env_vars.insert("POSTGRES_HOST_AUTH_METHOD".to_owned(), "trust".to_owned());

        Self { env_vars, copy_to_sources: Vec::new() }
    }
}

impl Image for Postgres {
    fn name(&self) -> &str {
        NAME
    }

    fn tag(&self) -> &str {
        TAG
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![
            WaitFor::message_on_stderr("database system is ready to accept connections"),
            WaitFor::message_on_stdout("database system is ready to accept connections"),
        ]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn copy_to_sources(&self) -> impl IntoIterator<Item = &CopyToContainer> {
        &self.copy_to_sources
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
        vec![
            "postgres",
            "-c",
            "fsync=off",
            "-c",
            "synchronous_commit=off",
            "-c",
            "full_page_writes=off",
            "-c",
            "wal_level=minimal",
            "-c",
            "max_wal_senders=0",
            "-c",
            "archive_mode=off",
            "-c",
            "summarize_wal=off",
            "-c",
            "autovacuum=off",
            "-c",
            "timescaledb.max_background_workers=0",
            "-c",
            "random_page_cost=1.1",
        ]
    }

    fn expose_ports(&self) -> &[testcontainers::core::ContainerPort] {
        &[testcontainers::core::ContainerPort::Tcp(5432)]
    }
}
