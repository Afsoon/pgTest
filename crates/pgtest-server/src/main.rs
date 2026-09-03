use envconfig::Envconfig;
use pgtest::postgres_manager::{PostgresConfig, PostgresManager};

#[derive(Envconfig, Debug)]
struct Config {
    #[envconfig(from = "PGTEST_WIRE_PORT", default = "2345")]
    pub pgtest_wire_port: u16,
    #[envconfig(from = "PGTEST_INITIAL_POOL_SIZE", default = "16")]
    pub pgtest_initial_pool_size: u16,
    #[envconfig(from = "PGTEST_MAXIMUM_POOL_SIZE", default = "96")]
    pub pgtest_maximum_pool_size: u16,

    #[envconfig(nested)]
    db_config: PostgresConfig,
}

fn main() {
    let config = match Config::init_from_env() {
        Ok(config_resolved) => config_resolved,
        Err(config_error) => {
            eprint!("error parrsing config {config_error}");
            // TODO: Nice log using tracing
            panic!("Invalid config provided")
        }
    };

    let postgres_client = PostgresManager::start(&config_unwrapped.db_config).await;

    println!("Hello, world!");
}
