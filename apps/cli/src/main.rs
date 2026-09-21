mod args;
mod server;
mod version;

use anyhow::{Context, Result};
use tracing_subscriber::{EnvFilter, prelude::*};

#[cfg(not(feature = "hotpath-alloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<()> {
    let options = match args::options().run() {
        args::Command::Serve(options) => options,
        args::Command::Version => {
            println!("Version: {}", version::display());
            return Ok(());
        }
    };
    options.validate()?;
    let filter = EnvFilter::try_new(&options.log_filter).context("invalid --log-filter")?;
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(std::io::stderr)
            .with_filter(filter),
    );
    #[cfg(feature = "hotpath")]
    let subscriber = subscriber.with(hotpath::sqlx_tracing_layer());
    subscriber.init();
    run(options)
}

#[tokio::main]
#[hotpath::main(allocator = mimalloc::MiMalloc)]
async fn run(options: args::ServeOptions) -> Result<()> {
    hotpath::tokio_runtime!();
    server::serve(options).await
}
