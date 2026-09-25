use std::collections::HashSet;

use pgtest::worker_engine::core::LeaseId;
use tokio::{sync::Barrier, task::JoinSet};

use super::*;

struct Harness {
    warmer: ConnectionWarmer,
    manager: Arc<WorkerEngineManager>,
    template: String,
    tcp: TcpWireListener,
    #[cfg(unix)]
    unix: wire_listener::UnixWireListener,
    #[cfg(unix)]
    directory: std::path::PathBuf,
    admin: Client,
    admin_task: JoinHandle<Result<(), tokio_postgres::Error>>,
}

impl Harness {
    async fn new(expiry_ms: u64) -> Self {
        let mut warmer = ConnectionWarmer::new(config(2, 4, Duration::from_secs(5)), "postgres");
        let pg = pg_container_config().await;
        let template = pg.pgtest_pg_database.clone();
        let manager = Arc::new(
            WorkerEngineManager::start_with_lifecycle(
                pg,
                WorkerEngineConfig {
                    initial_slots: 2,
                    grow_batch_size: 0,
                    lease_claim_timeout_ms: expiry_ms,
                    ..Default::default()
                },
                warmer.lifecycle(),
            )
            .await
            .unwrap(),
        );
        assert_eq!(warmer.start(&manager).await, WarmStartupOutcome::Ready { connections: 4 });
        let tcp = warmer.listen_tcp(manager.clone(), ([127, 0, 0, 1], 0).into()).await.unwrap();
        #[cfg(unix)]
        let directory = std::env::temp_dir().join(format!(
            "pgint-{}-{}",
            std::process::id(),
            tcp.local_addr().port()
        ));
        #[cfg(unix)]
        std::fs::create_dir(&directory).unwrap();
        #[cfg(unix)]
        let unix = warmer.listen_unix(manager.clone(), &directory, 6432).await.unwrap();
        let (admin, connection) = Config::new()
            .host(&manager.pg_client.host)
            .port(manager.pg_client.port)
            .user("postgres")
            .dbname("postgres")
            .connect(NoTls)
            .await
            .unwrap();
        let admin_task = tokio::spawn(connection);
        Self {
            warmer,
            manager,
            template,
            tcp,
            #[cfg(unix)]
            unix,
            #[cfg(unix)]
            directory,
            admin,
            admin_task,
        }
    }

    fn client_config(&self, lease: &str, unix: bool) -> Config {
        let mut config = Config::new();
        config.user("postgres").dbname(format!("{}/{lease}", self.template));
        #[cfg(unix)]
        if unix {
            config.host_path(&self.directory).port(6432);
            return config;
        }
        let _ = unix;
        config.host("127.0.0.1").port(self.tcp.local_addr().port());
        config
    }

    fn pool(&self) -> &Arc<ConnectionWarmPool> {
        self.warmer.pool.as_ref().unwrap()
    }

    async fn initial_pids(&self) -> HashSet<i32> {
        let names: Vec<_> = self
            .pool()
            .state
            .lock()
            .unwrap()
            .databases
            .values()
            .map(|e| e.database_name.clone())
            .collect();
        self.admin
            .query("SELECT pid FROM pg_stat_activity WHERE datname = ANY($1)", &[&names])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect()
    }

    async fn deleted(&self, name: &str) {
        while self
            .admin
            .query_one("SELECT EXISTS (SELECT FROM pg_database WHERE datname = $1)", &[&name])
            .await
            .unwrap()
            .get::<_, bool>(0)
        {
            tokio::task::yield_now().await;
        }
    }

    async fn stop(mut self) {
        self.tcp.shutdown().await;
        #[cfg(unix)]
        {
            self.unix.shutdown().await;
            std::fs::remove_dir(self.directory).unwrap();
        }
        self.warmer.shutdown().await.unwrap();
        assert_eq!(self.warmer.pool.as_ref().unwrap().state.lock().unwrap().capacity_used, 0);
        stop_manager(self.manager).await;
        drop(self.admin);
        self.admin_task.await.unwrap().unwrap();
    }
}

async fn connect(config: Config) -> (Client, JoinHandle<Result<(), tokio_postgres::Error>>) {
    let (client, connection) = config.connect(NoTls).await.unwrap();
    (client, tokio::spawn(connection))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_listener_bursts_use_unique_backends_and_keep_databases_isolated() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let h = Harness::new(0).await;
        let initial = h.initial_pids().await;
        assert_eq!(initial.len(), 4);
        let barrier = Arc::new(Barrier::new(13));
        let mut clients = JoinSet::new();
        for index in 0..12 {
            let lease = if index % 2 == 0 { "alpha" } else { "beta" };
            let config = h.client_config(lease, index % 3 == 0);
            let barrier = barrier.clone();
            clients.spawn(async move {
                barrier.wait().await;
                let (client, task) = connect(config).await;
                let row = client
                    .query_one("SELECT current_database(), pg_backend_pid()", &[])
                    .await
                    .unwrap();
                (lease, row.get::<_, String>(0), row.get::<_, i32>(1), client, task)
            });
        }
        barrier.wait().await;
        let mut sessions = Vec::new();
        let mut pids = HashSet::new();
        let mut databases = BTreeMap::new();
        while let Some(result) = clients.join_next().await {
            let (lease, name, pid, client, task) = result.unwrap();
            assert!(pids.insert(pid), "one backend must never serve two clients");
            assert_eq!(databases.entry(lease).or_insert(name.clone()), &name);
            let state = h.pool().state.lock().unwrap();
            assert!(state.capacity_used <= 4);
            assert!(state.databases.values().map(|e| e.in_flight).sum::<usize>() <= 2);
            assert!(
                state.databases.values().all(|e| e.checked_out + e.idle.len() + e.in_flight <= 2)
            );
            drop(state);
            sessions.push((lease, client, task));
        }
        assert!(initial.is_subset(&pids), "both leases must consume their initial spares");
        assert_ne!(databases["alpha"], databases["beta"]);
        let alpha = &sessions.iter().find(|s| s.0 == "alpha").unwrap().1;
        let beta = &sessions.iter().find(|s| s.0 == "beta").unwrap().1;
        alpha
            .batch_execute(
                "CREATE TABLE isolation_marker(value int); INSERT INTO isolation_marker VALUES \
                 (42)",
            )
            .await
            .unwrap();
        assert!(
            beta.query_one("SELECT to_regclass('public.isolation_marker')::text", &[])
                .await
                .unwrap()
                .get::<_, Option<String>>(0)
                .is_none()
        );
        for (_, client, task) in sessions {
            drop(client);
            task.await.unwrap().unwrap();
        }
        for (lease, name) in databases {
            h.manager.release(LeaseId::new(lease).unwrap()).await.unwrap();
            h.deleted(&name).await;
        }
        h.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn used_sessions_never_return_with_temp_tables_settings_or_transactions() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let h = Harness::new(0).await;
        let (first, task) = connect(h.client_config("state", false)).await;
        let pid = backend(&first).await;
        first
            .batch_execute(
                "CREATE TEMP TABLE private_temp(v int); SET application_name = 'dirty'; PREPARE \
                 private_plan AS SELECT 1; CREATE TABLE tx_marker(v int)",
            )
            .await
            .unwrap();
        first.batch_execute("BEGIN; INSERT INTO tx_marker VALUES (7)").await.unwrap();
        drop(first);
        task.await.unwrap().unwrap();
        let (second, task) = connect(h.client_config("state", true)).await;
        assert_ne!(pid, backend(&second).await);
        let row = second
            .query_one(
                "SELECT to_regclass('pg_temp.private_temp')::text, \
                 current_setting('application_name'), EXISTS (SELECT FROM pg_prepared_statements \
                 WHERE name = 'private_plan'), (SELECT count(*) FROM tx_marker)",
                &[],
            )
            .await
            .unwrap();
        assert!(row.get::<_, Option<String>>(0).is_none());
        assert_eq!(row.get::<_, String>(1), "");
        assert!(!row.get::<_, bool>(2));
        assert_eq!(row.get::<_, i64>(3), 0);
        drop(second);
        task.await.unwrap().unwrap();
        h.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_racing_both_listeners_closes_handoffs_and_drains_the_database() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let h = Harness::new(0).await;
        let lease = h.manager.attach(&h.template, LeaseId::new("race").unwrap()).await.unwrap();
        let (active, active_task) = connect(h.client_config("race", false)).await;
        let barrier = Arc::new(Barrier::new(9));
        let mut clients = JoinSet::new();
        for index in 0..8 {
            let config = h.client_config("race", index % 2 == 0);
            let barrier = barrier.clone();
            clients.spawn(async move {
                barrier.wait().await;
                match config.connect(NoTls).await {
                    Ok((client, connection)) => {
                        let task = tokio::spawn(connection);
                        Some((client, task))
                    }
                    Err(_) => None, // Retirement may win at attach, checkout, or startup.
                }
            });
        }
        barrier.wait().await;
        h.manager.release(lease.lease_id.clone()).await.unwrap();
        let _ = active_task.await.unwrap();
        assert!(active.is_closed());
        while let Some(result) = clients.join_next().await {
            if let Some((client, task)) = result.unwrap() {
                let _ = task.await.unwrap();
                assert!(client.is_closed());
            }
        }
        h.deleted(&lease.database_name).await;
        {
            let state = h.pool().state.lock().unwrap();
            let retired = &state.databases[&lease.database_id];
            assert!(retired.retiring && retired.idle.is_empty() && retired.in_flight == 0);
        }
        let error = h.client_config("race", true).connect(NoTls).await.err().unwrap();
        assert_eq!(error.code().unwrap().code(), "55000");
        let (other, task) = connect(h.client_config("unaffected", true)).await;
        assert!(backend(&other).await > 0);
        drop(other);
        task.await.unwrap().unwrap();
        drop(lease);
        h.stop().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn lease_expiry_closes_active_sessions_and_idle_spares_before_deletion() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let h = Harness::new(60_000).await;
        let lease = h.manager.attach(&h.template, LeaseId::new("expiry").unwrap()).await.unwrap();
        let (client, task) = connect(h.client_config("expiry", true)).await;
        assert!(backend(&client).await > 0);
        {
            let state = h.pool().state.lock().unwrap();
            let entry = &state.databases[&lease.database_id];
            assert_eq!(entry.checked_out, 1);
            assert_eq!(entry.idle.len(), 1);
        }
        // Advance only the already-armed lease timer; keep socket/DDL work on
        // real time so the outer timeout doesn't auto-advance during network
        // I/O.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::time::resume();
        let _ = task.await.unwrap();
        assert!(client.is_closed());
        h.deleted(&lease.database_name).await;
        // Expiry allows lease-ID reuse; explicit release closes it permanently.
        // Reuse must attach to a fresh physical identity, never old spares.
        let (renewed, renewed_task) = connect(h.client_config("expiry", false)).await;
        let renewed_lease =
            h.manager.attach(&h.template, LeaseId::new("expiry").unwrap()).await.unwrap();
        assert_ne!(renewed_lease.database_id, lease.database_id);
        let database: String =
            renewed.query_one("SELECT current_database()", &[]).await.unwrap().get(0);
        assert_ne!(database, lease.database_name.as_ref());
        assert!(h.pool().state.lock().unwrap().databases[&lease.database_id].retiring);
        drop(lease);
        assert!(
            backend(&renewed).await > 0,
            "an old-generation detach must preserve the new lease"
        );
        drop(renewed);
        renewed_task.await.unwrap().unwrap();
        drop(renewed_lease);
        h.stop().await;
    })
    .await
    .unwrap();
}
