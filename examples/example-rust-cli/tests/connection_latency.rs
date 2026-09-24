#![cfg(unix)]

mod support;

use std::{
    net::SocketAddr,
    panic::AssertUnwindSafe,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures_util::FutureExt;
use support::{PgTest, Session, with_lease};
use tokio::time::timeout;

#[tokio::test]
async fn connection_latency_baseline() -> Result<()> {
    let pgtest = PgTest::start().await?;
    let result =
        AssertUnwindSafe(timeout(Duration::from_secs(60), measure_connections(pgtest.address)))
            .catch_unwind()
            .await;
    let stopped = pgtest.stop().await;
    match result {
        Ok(result) => result.context("connection-latency workload exceeded 60 seconds")??,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    stopped
}

async fn measure_connections(address: SocketAddr) -> Result<()> {
    with_lease(address, async |_client, config| {
        // The helper has already assigned this lease. Measure connection
        // establishment separately from initial database assignment and
        // cleanup.
        let mut samples = Vec::with_capacity(100);
        for sample in 1..=100 {
            let started = Instant::now();
            let connected = Session::connect(config).await;
            let elapsed = started.elapsed();
            let session = connected.with_context(|| format!("connect sample {sample}/100"))?;
            samples.push(elapsed);
            drop(session);
        }

        samples.sort_unstable();
        // Nearest-rank percentiles for exactly 100 successful samples.
        println!(
            "connection_latency_baseline workload=sequential_same_lease samples={} p50_ms={:.6} \
             p95_ms={:.6} p99_ms={:.6}",
            samples.len(),
            samples[49].as_secs_f64() * 1000.0,
            samples[94].as_secs_f64() * 1000.0,
            samples[98].as_secs_f64() * 1000.0,
        );
        Ok(())
    })
    .await
}
