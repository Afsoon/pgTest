#![cfg(unix)]

mod support;

use std::{net::SocketAddr, panic::AssertUnwindSafe, time::Duration};

use anyhow::Result;
use example_rust_cli::{User, create_user, list_users};
use futures_util::FutureExt;
use support::{PgTest, Session, with_lease};
use tokio::time::timeout;

#[tokio::test]
async fn users_are_isolated_by_lease() -> Result<()> {
    let pgtest = PgTest::start().await?;
    let results = AssertUnwindSafe(timeout(Duration::from_secs(30), scenarios(pgtest.address)))
        .catch_unwind()
        .await;
    let stopped = pgtest.stop().await;
    match results {
        Ok(result) => result??,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    stopped
}

async fn scenarios(address: SocketAddr) -> Result<()> {
    let (ada, grace) = tokio::join!(
        tokio::spawn(isolated_user(address, "Ada")),
        tokio::spawn(isolated_user(address, "Grace")),
    );
    ada??;
    grace??;

    with_lease(address, async |client, config| {
        create_user(client, 1, "Shared user").await?;
        let second = Session::connect(config).await?;
        assert_eq!(
            list_users(&second.client).await?,
            vec![seed_user(), User { id: 1, name: "Shared user".into() }],
        );
        Ok(())
    })
    .await
}

async fn isolated_user(address: SocketAddr, name: &str) -> Result<()> {
    with_lease(address, async |client, _config| {
        assert_eq!(list_users(client).await?, vec![seed_user()]);
        assert_eq!(create_user(client, 1, name).await?, User { id: 1, name: name.into() });
        assert_eq!(list_users(client).await?, vec![seed_user(), User { id: 1, name: name.into() }]);
        Ok(())
    })
    .await
}

fn seed_user() -> User {
    User { id: 0, name: "Seed user".into() }
}
