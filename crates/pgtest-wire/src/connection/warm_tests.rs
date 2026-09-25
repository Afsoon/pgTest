use std::collections::BTreeMap;

use bytes::BytesMut;
use pgtest::{worker_engine::core::WorkerEngineConfig, worker_manager::worker_io::LeaseSession};
use pgtest_database_operations::testcontainer::pg_container_config;
use pgwire::messages::Message;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    task::{JoinHandle, JoinSet},
};
use tokio_postgres::{Client, Config, NoTls};

use super::*;
use crate::connection_warm::ConnectionWarmConfig;

type ClientTask = JoinHandle<Result<(), tokio_postgres::Error>>;

struct Harness {
    manager: Arc<WorkerEngineManager>,
    pool: Arc<ConnectionWarmPool>,
    handlers: JoinSet<()>,
    template: String,
}

fn profile() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("user".into(), "postgres".into()),
        ("client_encoding".into(), "UTF8".into()),
        ("application_name".into(), "vitest".into()),
        ("options".into(), "-c search_path=public".into()),
    ])
}

impl Harness {
    async fn new() -> Self {
        let config = pg_container_config().await;
        let template = config.pgtest_pg_database.clone();
        let manager = Arc::new(
            WorkerEngineManager::start(
                config,
                WorkerEngineConfig { initial_slots: 2, grow_batch_size: 0, ..Default::default() },
            )
            .await
            .unwrap(),
        );
        let pool = Arc::new(ConnectionWarmPool::new(
            ConnectionWarmConfig::try_new(1, 2, 1, Duration::ZERO, profile()).unwrap(),
            "postgres",
        ));
        Self { manager, pool, handlers: JoinSet::new(), template }
    }

    async fn attach(&self, lease: &str) -> LeaseSession {
        self.manager.attach(&self.template, LeaseId::new(lease).unwrap()).await.unwrap()
    }

    fn config(&self, lease: &str) -> Config {
        let mut config = Config::new();
        config
            .user("postgres")
            .dbname(format!("{}/{lease}", self.template))
            .application_name("vitest")
            .options("-c search_path=public");
        config
    }

    async fn seed(&self, lease: &LeaseSession) -> i32 {
        self.pool.register_database(lease.database_id, lease.database_name.to_string());
        let reservation = self.pool.try_reserve(lease.database_id).unwrap();
        let session = postgres_upstream::connect(
            &lease.database_name,
            &profile(),
            &self.manager.pg_client.host,
            self.manager.pg_client.port,
        )
        .await
        .unwrap();
        let pid = backend_pid(session.session_burst.bytes());
        assert!(reservation.publish(session));
        pid
    }

    async fn tcp(&mut self, pool: Option<Arc<ConnectionWarmPool>>) -> TcpStream {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let (client, accepted) =
            tokio::join!(TcpStream::connect(listener.local_addr().unwrap()), listener.accept(),);
        self.handlers.spawn(handle_connection(
            ClientStream::Tcp(accepted.unwrap().0),
            self.manager.clone(),
            pool,
        ));
        client.unwrap()
    }

    async fn connect(
        &mut self,
        config: &Config,
    ) -> Result<(Client, ClientTask), tokio_postgres::Error> {
        let stream = self.tcp(Some(self.pool.clone())).await;
        client_on(config, stream).await
    }

    async fn shutdown(mut self) {
        self.handlers.shutdown().await;
        drop(self.pool);
        Arc::try_unwrap(self.manager)
            .unwrap_or_else(|_| panic!("handler retained manager"))
            .shutdown()
            .await;
    }
}

async fn client_on<S>(
    config: &Config,
    stream: S,
) -> Result<(Client, ClientTask), tokio_postgres::Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (client, connection) = config.connect_raw(stream, NoTls).await?;
    Ok((client, tokio::spawn(connection)))
}

fn backend_pid(mut burst: &[u8]) -> i32 {
    while !burst.is_empty() {
        let length = i32::from_be_bytes(burst[1..5].try_into().unwrap()) as usize;
        if burst[0] == b'K' {
            return i32::from_be_bytes(burst[5..9].try_into().unwrap());
        }
        burst = &burst[length + 1..];
    }
    panic!("startup must contain BackendKeyData");
}

async fn pid(client: &Client) -> i32 {
    client.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0)
}

async fn close(client: Client, task: ClientTask) {
    drop(client);
    task.await.unwrap().unwrap();
}

async fn warm_handoff(unix: bool) {
    let mut h = Harness::new().await;
    let lease = h.attach("warm").await; // Keep the lease alive between clients.
    let warmed_pid = h.seed(&lease).await;
    let config = h.config("warm");
    let (client, task) = if unix {
        #[cfg(unix)]
        {
            let (client, server) = tokio::net::UnixStream::pair().unwrap();
            h.handlers.spawn(handle_connection(
                ClientStream::Unix(server),
                h.manager.clone(),
                Some(h.pool.clone()),
            ));
            client_on(&config, client).await.unwrap()
        }
        #[cfg(not(unix))]
        unreachable!()
    } else {
        h.connect(&config).await.unwrap()
    };
    let row = client
        .query_one(
            "SELECT pg_backend_pid(), current_database(), current_setting('application_name'), \
             current_setting('search_path')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), warmed_pid, "must use the prepared backend");
    assert_eq!(row.get::<_, &str>(1), lease.database_name.as_ref());
    assert_eq!(row.get::<_, &str>(2), "vitest");
    assert_eq!(row.get::<_, &str>(3), "public");
    assert!(h.pool.try_checkout(lease.database_id, &profile()).is_none());
    client.batch_execute("SET application_name = 'client-mutated'").await.unwrap();
    close(client, task).await;

    // Even with replenishment reserved but unfinished, a miss connects cold.
    let pending = h.pool.try_reserve(lease.database_id).unwrap();
    let (client, task) = h.connect(&config).await.unwrap();
    assert_ne!(pid(&client).await, warmed_pid, "a used session must never return to the pool");
    assert_eq!(
        client.query_one("SHOW application_name", &[]).await.unwrap().get::<_, &str>(0),
        "vitest"
    );
    // Release still closes a handed-off session through the existing relay.
    h.manager.release(lease.lease_id.clone()).await.unwrap();
    let _ = task.await.unwrap();
    assert!(client.is_closed());
    drop(client);
    drop(pending);
    drop(lease);
    h.shutdown().await;
}

#[tokio::test]
async fn warm_tcp_handoff_is_single_use_and_miss_does_not_wait() {
    tokio::time::timeout(Duration::from_secs(30), warm_handoff(false)).await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn warm_unix_handoff_uses_the_same_pool_and_relay() {
    tokio::time::timeout(Duration::from_secs(30), warm_handoff(true)).await.unwrap();
}

#[tokio::test]
async fn fallback_preserves_client_parameters_and_other_database_spares() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut h = Harness::new().await;
        let lease = h.attach("one").await;
        let other = h.attach("two").await;
        let warm_pid = h.seed(&lease).await;

        let mut mismatch = h.config("one");
        mismatch.application_name("different").options("-c search_path=pg_catalog");
        let (client, task) = h.connect(&mismatch).await.unwrap();
        assert_ne!(pid(&client).await, warm_pid);
        let row = client
            .query_one(
                "SELECT current_setting('application_name'), current_setting('search_path')",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, &str>(0), "different");
        assert_eq!(row.get::<_, &str>(1), "pg_catalog");
        close(client, task).await;

        let (client, task) = h.connect(&h.config("two")).await.unwrap();
        assert_ne!(pid(&client).await, warm_pid);
        assert_eq!(
            client.query_one("SELECT current_database()", &[]).await.unwrap().get::<_, &str>(0),
            other.database_name.as_ref()
        );
        close(client, task).await;

        // No configured pool also follows the unchanged cold path.
        let stream = h.tcp(None).await;
        let (client, task) = client_on(&h.config("one"), stream).await.unwrap();
        assert_ne!(pid(&client).await, warm_pid);
        close(client, task).await;

        // All three misses left the exact matching spare untouched.
        let (client, task) = h.connect(&h.config("one")).await.unwrap();
        assert_eq!(pid(&client).await, warm_pid);
        close(client, task).await;
        drop(other);
        drop(lease);
        h.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rejected_and_control_connections_do_not_consume_spares() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut h = Harness::new().await;
        let lease = h.attach("one").await;
        let warm_pid = h.seed(&lease).await;
        for (database, code) in
            [("invalid".to_owned(), "22023"), ("unknown/lease".to_owned(), "3D000")]
        {
            let mut config = h.config("one");
            config.dbname(database);
            let error = h.connect(&config).await.err().expect("connection must be rejected");
            assert_eq!(error.as_db_error().unwrap().code().code(), code);
        }
        let mut replication = h.tcp(Some(h.pool.clone())).await;
        let mut startup = Startup::new();
        startup.parameters = profile();
        startup.parameters.insert("database".into(), format!("{}/one", h.template));
        startup.parameters.insert("replication".into(), "true".into());
        let mut bytes = BytesMut::new();
        startup.encode(&mut bytes).unwrap();
        replication.write_all(&bytes).await.unwrap();
        assert_eq!(replication.read_u8().await.unwrap(), b'E');
        let length = replication.read_u32().await.unwrap();
        let mut error = vec![0; length as usize - 4];
        replication.read_exact(&mut error).await.unwrap();
        assert!(error.windows(7).any(|field| field == b"C0A000\0"));
        drop(replication);

        let mut control = h.config("one");
        control.dbname("pgtest");
        let (client, task) = h.connect(&control).await.unwrap();
        assert_eq!(client.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0), 1);
        close(client, task).await;
        let (client, task) = h.connect(&h.config("one")).await.unwrap();
        assert_eq!(pid(&client).await, warm_pid);
        close(client, task).await;

        // The handler must attach successfully before touching the pool.
        // Use another physical database: the first one's warm budget is spent.
        let closed_lease = h.attach("closed").await;
        h.seed(&closed_lease).await;
        h.manager.release(closed_lease.lease_id.clone()).await.unwrap();
        let error =
            h.connect(&h.config("closed")).await.err().expect("closed lease must be rejected");
        assert_eq!(error.as_db_error().unwrap().code().code(), "55000");
        assert!(
            h.pool.try_reserve(closed_lease.database_id).is_none(),
            "rejected attach must leave the spare occupying its slot"
        );
        drop(closed_lease);
        drop(lease);
        h.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn warm_handoff_preserves_pipelined_client_bytes_and_lease_cancellation() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut h = Harness::new().await;
        let lease = h.attach("pipeline").await;
        let warm_pid = h.seed(&lease).await;
        let mut client = h.tcp(Some(h.pool.clone())).await;
        let mut startup = Startup::new();
        startup.parameters = profile();
        startup.parameters.insert("database".into(), format!("{}/pipeline", h.template));
        let mut bytes = BytesMut::new();
        startup.encode(&mut bytes).unwrap();
        pgwire::messages::simplequery::Query::new("SELECT pg_backend_pid()".into())
            .encode(&mut bytes)
            .unwrap();
        client.write_all(&bytes).await.unwrap();
        let mut ready = 0;
        let mut authentication = 0;
        let mut reported_pid = None;
        let mut key_pid = None;
        while ready != 2 {
            let tag = client.read_u8().await.unwrap();
            let length = client.read_u32().await.unwrap();
            let mut payload = vec![0; length as usize - 4];
            client.read_exact(&mut payload).await.unwrap();
            match tag {
                b'R' => {
                    authentication += 1;
                    assert_eq!(payload, [0; 4]);
                }
                b'K' => key_pid = Some(i32::from_be_bytes(payload[..4].try_into().unwrap())),
                b'D' => {
                    reported_pid =
                        Some(std::str::from_utf8(&payload[6..]).unwrap().parse::<i32>().unwrap())
                }
                b'Z' => ready += 1,
                b'E' => panic!("unexpected error: {payload:?}"),
                _ => {}
            }
        }
        assert_eq!(authentication, 1, "warm startup must not duplicate AuthenticationOk");
        assert_eq!(key_pid, Some(warm_pid));
        assert_eq!(reported_pid, Some(warm_pid));
        h.manager.release(lease.lease_id.clone()).await.unwrap();
        assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
        drop(lease);
        h.shutdown().await;
    })
    .await
    .unwrap();
}
