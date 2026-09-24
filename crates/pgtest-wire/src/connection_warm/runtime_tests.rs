use pgtest::worker_engine::core::WorkerEngineConfig;
use pgtest_database_operations::testcontainer::pg_container_config;
use tokio_postgres::{Client, Config, NoTls};

use super::*;

#[path = "integration_tests.rs"]
mod integration;

fn config(count: u16, cap: usize, wait: Duration) -> ConnectionWarmConfig {
    ConnectionWarmConfig::try_new(
        count,
        cap,
        2,
        wait,
        BTreeMap::from([("client_encoding".into(), "UTF8".into())]),
    )
    .unwrap()
}

async fn start_manager(
    warmer: &ConnectionWarmer,
    slots: u16,
) -> (Arc<WorkerEngineManager>, String) {
    let pg = pg_container_config().await;
    let template = pg.pgtest_pg_database.clone();
    let manager = WorkerEngineManager::start_with_lifecycle(
        pg,
        WorkerEngineConfig { initial_slots: slots, grow_batch_size: 1, ..Default::default() },
        warmer.lifecycle(),
    )
    .await
    .unwrap();
    (Arc::new(manager), template)
}

fn inventory(pool: &ConnectionWarmPool) -> usize {
    pool.state.lock().unwrap().databases.values().map(|entry| entry.idle.len()).sum()
}

async fn backend(client: &Client) -> i32 {
    client.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0)
}

async fn stop_manager(manager: Arc<WorkerEngineManager>) {
    Arc::try_unwrap(manager).unwrap_or_else(|_| panic!("manager retained")).shutdown().await;
}

#[tokio::test]
async fn bounded_startup_caps_inventory_and_shares_warm_backends_between_listeners() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut warmer = ConnectionWarmer::new(config(4, 2, Duration::from_secs(5)), "postgres");
        let (manager, template) = start_manager(&warmer, 1).await;
        assert_eq!(warmer.start(&manager).await, WarmStartupOutcome::Ready { connections: 2 });
        let pool = warmer.pool.as_ref().unwrap().clone();
        assert_eq!(inventory(&pool), 2);
        let names: Vec<String> = pool
            .state
            .lock()
            .unwrap()
            .databases
            .values()
            .map(|entry| entry.database_name.clone())
            .collect();
        let (admin, connection) = Config::new()
            .host(&manager.pg_client.host)
            .port(manager.pg_client.port)
            .user("postgres")
            .dbname("postgres")
            .connect(NoTls)
            .await
            .unwrap();
        let admin_task = tokio::spawn(connection);
        let initial: Vec<i32> = admin
            .query("SELECT pid FROM pg_stat_activity WHERE datname = ANY($1)", &[&names])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(initial.len(), 2);
        let tcp = warmer.listen_tcp(manager.clone(), ([127, 0, 0, 1], 0).into()).await.unwrap();
        let mut client_config = Config::new();
        client_config
            .host("127.0.0.1")
            .port(tcp.local_addr().port())
            .user("postgres")
            .dbname(format!("{template}/startup"));
        let (first, connection) = client_config.connect(NoTls).await.unwrap();
        let first_task = tokio::spawn(connection);
        let first_pid = backend(&first).await;
        assert!(initial.contains(&first_pid), "TCP must use an initial spare");

        #[cfg(unix)]
        let directory = {
            // The ephemeral TCP port makes paths unique across parallel tests.
            let path = std::env::temp_dir().join(format!(
                "pgwarm-{}-{}",
                std::process::id(),
                tcp.local_addr().port()
            ));
            std::fs::create_dir(&path).unwrap();
            path
        };
        #[cfg(unix)]
        let unix = warmer.listen_unix(manager.clone(), &directory, 6432).await.unwrap();
        #[cfg(unix)]
        {
            client_config = Config::new();
            client_config
                .host_path(&directory)
                .port(6432)
                .user("postgres")
                .dbname(format!("{template}/startup"));
        }
        let (second, connection) = client_config.connect(NoTls).await.unwrap();
        let second_task = tokio::spawn(connection);
        let second_pid = backend(&second).await;
        assert!(initial.contains(&second_pid), "both listeners must share initial inventory");
        assert_ne!(first_pid, second_pid);

        // Binding failure must still allow the owner to drain existing work.
        assert!(warmer.listen_tcp(manager.clone(), tcp.local_addr()).await.is_err());
        tcp.shutdown().await;
        #[cfg(unix)]
        {
            unix.shutdown().await;
            std::fs::remove_dir(directory).unwrap();
        }
        let _ = first_task.await.unwrap();
        let _ = second_task.await.unwrap();
        assert!(first.is_closed() && second.is_closed());
        warmer.shutdown().await.unwrap();
        assert_eq!(inventory(&pool), 0);
        assert_eq!(pool.state.lock().unwrap().capacity_used, 0);
        stop_manager(manager).await;
        drop(admin);
        admin_task.await.unwrap().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn background_startup_returns_before_inventory_and_then_replenishes() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut warmer = ConnectionWarmer::new(config(1, 1, Duration::ZERO), "postgres");
        let (manager, _) = start_manager(&warmer, 1).await;
        assert_eq!(warmer.start(&manager).await, WarmStartupOutcome::BackgroundOnly);
        let pool = warmer.pool.as_ref().unwrap();
        // On this current-thread runtime start must not yield to its scheduler.
        assert_eq!(inventory(pool), 0);
        while inventory(pool) != 1 {
            tokio::task::yield_now().await;
        }
        warmer.shutdown().await.unwrap();
        stop_manager(manager).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn failed_warm_profile_hits_deadline_but_listener_connects_cold() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut settings = config(1, 1, Duration::from_millis(30));
        settings.startup_params.insert("user".into(), "nonexistent_warm_user".into());
        let mut warmer = ConnectionWarmer::new(settings, "postgres");
        let (manager, template) = start_manager(&warmer, 1).await;
        assert_eq!(
            warmer.start(&manager).await,
            WarmStartupOutcome::TimedOut { ready: 0, target: 1 }
        );
        let listener =
            warmer.listen_tcp(manager.clone(), ([127, 0, 0, 1], 0).into()).await.unwrap();
        let (client, connection) = Config::new()
            .host("127.0.0.1")
            .port(listener.local_addr().port())
            .user("postgres")
            .dbname(format!("{template}/cold"))
            .connect(NoTls)
            .await
            .unwrap();
        let task = tokio::spawn(connection);
        assert!(backend(&client).await > 0);
        drop(client);
        task.await.unwrap().unwrap();
        listener.shutdown().await;
        warmer.shutdown().await.unwrap();
        stop_manager(manager).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn empty_initial_population_returns_ready_and_runtime_creation_warms() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut warmer = ConnectionWarmer::new(config(1, 1, Duration::from_secs(5)), "postgres");
        let (manager, template) = start_manager(&warmer, 0).await;
        assert_eq!(warmer.start(&manager).await, WarmStartupOutcome::Ready { connections: 0 });
        let lease = manager
            .attach(&template, pgtest::worker_engine::core::LeaseId::new("later").unwrap())
            .await
            .unwrap();
        while inventory(warmer.pool.as_ref().unwrap()) != 1 {
            tokio::task::yield_now().await;
        }
        drop(lease);
        warmer.shutdown().await.unwrap();
        stop_manager(manager).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn interrupted_warm_startup_drops_owned_work_and_disabled_mode_allocates_none() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut disabled = ConnectionWarmer::new(ConnectionWarmConfig::default(), "postgres");
        let (manager, _) = start_manager(&disabled, 1).await;
        assert_eq!(disabled.start(&manager).await, WarmStartupOutcome::Disabled);
        assert!(disabled.pool.is_none() && disabled.task.is_none());
        disabled.shutdown().await.unwrap();
        stop_manager(manager).await;

        let mut warmer = ConnectionWarmer::new(config(1, 1, Duration::from_secs(5)), "postgres");
        let (manager, _) = start_manager(&warmer, 1).await;
        let pool = warmer.pool.as_ref().unwrap().clone();
        let mut startup = Box::pin(warmer.start(&manager));
        assert!(futures::poll!(startup.as_mut()).is_pending());
        drop(startup);
        drop(warmer);
        pool.shutdown().await.unwrap();
        assert!(pool.cancellation.is_cancelled());
        assert_eq!(pool.state.lock().unwrap().capacity_used, 0);
        stop_manager(manager).await;
    })
    .await
    .unwrap();
}
