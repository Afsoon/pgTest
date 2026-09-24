use std::future::poll_fn;

use bytes::BytesMut;
use pgtest::{
    worker_engine::core::{LeaseId, WorkerEngineConfig},
    worker_manager::WorkerEngineManager,
};
use pgtest_database_operations::testcontainer::pg_container_config;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use super::{
    test_support::{BURST, OTHER_BURST},
    *,
};
use crate::postgres_upstream::upstream_stream::UpstreamStream;

fn pool() -> Arc<ConnectionWarmPool> {
    let config = ConnectionWarmConfig::try_new(3, 3, 1, Duration::ZERO, BTreeMap::new()).unwrap();
    let pool = Arc::new(ConnectionWarmPool::new(config, "postgres"));
    pool.register_database(DatabaseId(1), "physical".into());
    pool
}

async fn pair(tcp: bool, burst: &[u8]) -> (UpstreamSession, UpstreamStream) {
    if !tcp {
        return super::test_support::ready_session(burst).await;
    }
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let (stream, peer) =
        tokio::join!(TcpStream::connect(listener.local_addr().unwrap()), listener.accept());
    (
        UpstreamSession {
            stream: UpstreamStream::Tcp(stream.unwrap()),
            session_burst: BytesMut::from(burst).into(),
        },
        UpstreamStream::Tcp(peer.unwrap().0),
    )
}

async fn readable(stream: &UpstreamStream) {
    match stream {
        UpstreamStream::Tcp(stream) => stream.readable().await.unwrap(),
        #[cfg(unix)]
        UpstreamStream::Unix(stream) => stream.readable().await.unwrap(),
    }
}

async fn closed(mut peer: UpstreamStream) {
    match peer.read(&mut [0]).await {
        Ok(count) => assert_eq!(count, 0),
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset),
    }
}

#[tokio::test]
async fn checkout_skips_closed_and_unsolicited_data_spares_without_monitor_or_query() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for tcp in [true, false] {
            let pool = pool();
            let (eof, mut eof_peer) = pair(tcp, BURST).await;
            eof_peer.shutdown().await.unwrap();
            readable(&eof.stream).await;
            assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(eof));

            let (error, mut error_peer) = pair(tcp, BURST).await;
            // A partial error frame must be discarded without awaiting its
            // body.
            error_peer.write_all(b"E\0").await.unwrap();
            readable(&error.stream).await;
            assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(error));

            let (healthy, mut healthy_peer) = pair(tcp, OTHER_BURST).await;
            assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(healthy));
            let mut session = pool.try_checkout(DatabaseId(1), pool.profile.parameters()).unwrap();
            assert_eq!(session.session_burst.bytes(), OTHER_BURST);
            assert_eq!(pool.state.lock().unwrap().capacity_used, 0);
            closed(eof_peer).await;
            closed(error_peer).await;
            assert!(
                futures::poll!(Box::pin(healthy_peer.read(&mut [0])).as_mut()).is_pending(),
                "health checks must not send SQL"
            );
            healthy_peer.write_all(b"client-owned").await.unwrap();
            let mut payload = [0; 12];
            session.stream.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, b"client-owned");
            drop(session);
            closed(healthy_peer).await;

            let (last, mut peer) = pair(tcp, BURST).await;
            peer.shutdown().await.unwrap();
            readable(&last.stream).await;
            assert!(pool.try_reserve(DatabaseId(1)).unwrap().publish(last));
            assert!(pool.try_checkout(DatabaseId(1), pool.profile.parameters()).is_none());
            assert_eq!(pool.state.lock().unwrap().capacity_used, 0);
            assert!(pool.try_reserve(DatabaseId(1)).is_some());
            closed(peer).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn terminated_postgres_spare_falls_back_cold_through_the_handler() {
    use crate::connection::{ClientStream, handle_connection};
    tokio::time::timeout(Duration::from_secs(30), async {
        let config = pg_container_config().await;
        let template = config.pgtest_pg_database.clone();
        let manager = Arc::new(
            WorkerEngineManager::start(
                config,
                WorkerEngineConfig { initial_slots: 1, grow_batch_size: 0, ..Default::default() },
            )
            .await
            .unwrap(),
        );
        let lease = manager.attach(&template, LeaseId::new("dead-spare").unwrap()).await.unwrap();
        let mut params = BTreeMap::from([
            ("user".to_owned(), "postgres".to_owned()),
            ("client_encoding".to_owned(), "UTF8".to_owned()),
        ]);
        let pool = Arc::new(ConnectionWarmPool::new(
            ConnectionWarmConfig::try_new(1, 1, 1, Duration::ZERO, params.clone()).unwrap(),
            "postgres",
        ));
        pool.register_database(lease.database_id, lease.database_name.to_string());
        let session = postgres_upstream::connect(
            &lease.database_name,
            &params,
            &manager.pg_client.host,
            manager.pg_client.port,
        )
        .await
        .unwrap();
        let mut burst = session.session_burst.bytes();
        let dead_pid = loop {
            let length = i32::from_be_bytes(burst[1..5].try_into().unwrap()) as usize;
            if burst[0] == b'K' {
                break i32::from_be_bytes(burst[5..9].try_into().unwrap());
            }
            burst = &burst[length + 1..];
        };
        assert!(session.stream.is_idle(), "fresh backend must be idle");
        assert!(pool.try_reserve(lease.database_id).unwrap().publish(session));
        let (admin, connection) = tokio_postgres::Config::new()
            .host(&manager.pg_client.host)
            .port(manager.pg_client.port)
            .user("postgres")
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        let admin_task = tokio::spawn(connection);
        assert!(
            admin
                .query_one("SELECT pg_terminate_backend($1)", &[&dead_pid])
                .await
                .unwrap()
                .get::<_, bool>(0)
        );
        // Wait for socket readiness, without consuming the error or running
        // the monitor: the client checkout itself must reject the dead spare.
        poll_fn(|cx| {
            let state = pool.state.lock().unwrap();
            let stream = &state.databases[&lease.database_id].idle[0].session.stream;
            match stream {
                UpstreamStream::Tcp(stream) => stream.poll_read_ready(cx),
                #[cfg(unix)]
                UpstreamStream::Unix(stream) => stream.poll_read_ready(cx),
            }
        })
        .await
        .unwrap();
        params.insert("database".into(), format!("{template}/dead-spare"));
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let (client, accepted) =
            tokio::join!(TcpStream::connect(listener.local_addr().unwrap()), listener.accept());
        let handler = tokio::spawn(handle_connection(
            ClientStream::Tcp(accepted.unwrap().0),
            manager.clone(),
            Some(pool.clone()),
        ));
        let (client, connection) = tokio_postgres::Config::new()
            .user("postgres")
            .dbname(&params["database"])
            .connect_raw(client.unwrap(), tokio_postgres::NoTls)
            .await
            .unwrap();
        let task = tokio::spawn(connection);
        let row =
            client.query_one("SELECT pg_backend_pid(), current_database()", &[]).await.unwrap();
        assert_ne!(row.get::<_, i32>(0), dead_pid);
        assert_eq!(row.get::<_, &str>(1), lease.database_name.as_ref());
        assert_eq!(pool.state.lock().unwrap().capacity_used, 0);
        drop(client);
        task.await.unwrap().unwrap();
        handler.await.unwrap();
        drop(admin);
        admin_task.await.unwrap().unwrap();
        drop(pool);
        drop(lease);
        Arc::try_unwrap(manager).unwrap_or_else(|_| panic!("manager retained")).shutdown().await;
    })
    .await
    .unwrap();
}
