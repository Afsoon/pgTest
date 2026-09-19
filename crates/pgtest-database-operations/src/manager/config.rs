use envconfig::Envconfig;

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
    #[envconfig(from = "PGTEST_CREATION_POOL_CONNECTION", default = "10")]
    pub pgtest_pg_creation_pool_connection: u32,
    #[envconfig(from = "PGTEST_CLEANUP_POOL_CONNECTION", default = "5")]
    pub pgtest_pg_cleanup_pool_connection: u32,
}
