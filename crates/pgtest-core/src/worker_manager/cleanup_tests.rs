use std::{
    future::poll_fn,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::Poll,
};

use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::worker_engine::errors::PostgresDDLClientError;

type DropResult = Result<(), PostgresDDLClientError>;

struct DropRequest {
    database_name: String,
    finish: oneshot::Sender<DropResult>,
}

struct ControlledPostgres {
    drops: mpsc::UnboundedSender<DropRequest>,
    creations: AtomicUsize,
    fail_create: AtomicBool,
}

impl PostgresClient for ControlledPostgres {
    async fn drop_database(&self, database_name: &str) -> DropResult {
        let (finish, result) = oneshot::channel();
        self.drops.send(DropRequest { database_name: database_name.into(), finish }).unwrap();
        result.await.expect("test must finish or cancel every drop")
    }

    async fn create_database(&self) -> Result<ReadString, PostgresDDLClientError> {
        self.creations.fetch_add(1, Ordering::SeqCst);
        if self.fail_create.load(Ordering::SeqCst) {
            Err(PostgresDDLClientError::NonRecoverableError("injected create failure".into()))
        } else {
            Ok(ReadString::from("replacement"))
        }
    }

    async fn drop_templates_like(&self) -> DropResult {
        unreachable!()
    }
}

struct Fixture {
    cleanup: DatabaseCleanup,
    client: Arc<ControlledPostgres>,
    drops: mpsc::UnboundedReceiver<DropRequest>,
    tracker: TaskTracker,
    shutdown: CancellationToken,
}

impl Fixture {
    fn new(config: WorkerEngineConfig) -> Self {
        let tracker = TaskTracker::new();
        let shutdown = CancellationToken::new();
        let cleanup = DatabaseCleanup::new(&config, tracker.clone(), shutdown.clone());
        let (drops, receiver) = mpsc::unbounded_channel();
        Self {
            cleanup,
            client: Arc::new(ControlledPostgres {
                drops,
                creations: AtomicUsize::new(0),
                fail_create: AtomicBool::new(false),
            }),
            drops: receiver,
            tracker,
            shutdown,
        }
    }

    async fn enqueue(&self, name: &str) {
        self.cleanup.enqueue(ReadString::from(name), self.client.clone()).await.unwrap();
    }

    async fn next_drop(&mut self) -> DropRequest {
        tokio::time::timeout(Duration::from_secs(1), self.drops.recv())
            .await
            .expect("a drop should have started")
            .expect("drop channel must be open")
    }

    async fn finish(&self) {
        self.tracker.close();
        tokio::time::timeout(Duration::from_secs(1), self.tracker.wait())
            .await
            .expect("all cleanup tasks should have completed");
    }
}

async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn replacement_retries_creation_without_waiting_for_drop() {
    for fail_create in [false, true] {
        let mut fixture = Fixture::new(WorkerEngineConfig::default());
        fixture.client.fail_create.store(fail_create, Ordering::SeqCst);
        let (sender, mut receiver) = hotpath::channel!(mpsc::unbounded_channel());
        let lease = LeaseId::from("old-lease");
        let worker_index = 18;
        let mut replacement = Box::pin(recreate_database(
            sender,
            worker_index,
            ReadString::from("old-database"),
            lease.clone(),
            7,
            fixture.client.clone(),
            fixture.cleanup.clone(),
        ));
        if fail_create {
            assert_pending(replacement.as_mut()).await;
        } else {
            replacement.as_mut().await;
        }
        let old_database = fixture.next_drop().await;
        assert_eq!(old_database.database_name, "old-database");
        assert!(matches!(receiver.recv().await,
            Some(EngineMessage::DeleteLease { lease: retired, generation: 7 }) if retired == lease));
        if fail_create {
            assert!(receiver.try_recv().is_err(), "failed creation must retain the pending job");
            assert_eq!(fixture.client.creations.load(Ordering::SeqCst), 1);
            fixture.client.fail_create.store(false, Ordering::SeqCst);
            tokio::time::advance(Duration::from_secs(1)).await;
            replacement.as_mut().await;
            assert_eq!(fixture.client.creations.load(Ordering::SeqCst), 2);
        }
        drop(replacement);
        let Some(EngineMessage::TemplateCreated { index, result }) = receiver.recv().await else {
            panic!("replacement must complete while the drop is still blocked");
        };
        assert_eq!(index, worker_index);
        assert!(result.is_ok());
        assert!(receiver.recv().await.is_none());
        old_database.finish.send(Ok(())).unwrap();
        fixture.finish().await;
    }
}

#[tokio::test]
async fn full_backlog_blocks_replacement_until_a_deletion_completes() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        cleanup_max_pending: 2,
        cleanup_concurrency: 1,
        ..WorkerEngineConfig::default()
    });
    fixture.enqueue("first").await;
    let first = fixture.next_drop().await;
    fixture.enqueue("second").await;

    let (sender, mut receiver) = hotpath::channel!(mpsc::unbounded_channel());
    let mut replacement = Box::pin(recreate_database(
        sender,
        0,
        ReadString::from("third"),
        LeaseId::from("old-lease"),
        1,
        fixture.client.clone(),
        fixture.cleanup.clone(),
    ));
    assert_pending(replacement.as_mut()).await;
    assert_eq!(fixture.client.creations.load(Ordering::SeqCst), 0);
    assert!(receiver.try_recv().is_err());
    assert!(fixture.drops.try_recv().is_err());

    first.finish.send(Ok(())).unwrap();
    replacement.await;
    assert_eq!(fixture.client.creations.load(Ordering::SeqCst), 1);
    let second = fixture.next_drop().await;
    assert_eq!(second.database_name, "second");
    second.finish.send(Ok(())).unwrap();
    let third = fixture.next_drop().await;
    assert_eq!(third.database_name, "third");
    third.finish.send(Ok(())).unwrap();
    fixture.finish().await;
}

#[tokio::test]
async fn concurrent_drops_never_exceed_the_configured_limit() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        cleanup_max_pending: 3,
        cleanup_concurrency: 2,
        ..WorkerEngineConfig::default()
    });
    fixture.enqueue("first").await;
    fixture.enqueue("second").await;
    fixture.enqueue("third").await;
    let first = fixture.next_drop().await;
    let second = fixture.next_drop().await;
    tokio::task::yield_now().await;
    assert!(fixture.drops.try_recv().is_err());

    first.finish.send(Ok(())).unwrap();
    let third = fixture.next_drop().await;
    assert_ne!(first.database_name, second.database_name);
    assert_ne!(second.database_name, third.database_name);
    assert_ne!(first.database_name, third.database_name);
    second.finish.send(Ok(())).unwrap();
    third.finish.send(Ok(())).unwrap();
    fixture.finish().await;
}

#[tokio::test(start_paused = true)]
async fn failures_keep_backlog_capacity_and_retry_with_a_capped_delay() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        cleanup_max_pending: 1,
        cleanup_concurrency: 1,
        ..WorkerEngineConfig::default()
    });
    fixture.enqueue("retry-me").await;
    let mut request = fixture.next_drop().await;
    let cleanup = fixture.cleanup.clone();
    let mut admission = Box::pin(cleanup.enqueue(ReadString::from("next"), fixture.client.clone()));

    for retry_after_secs in [1, 2, 4, 8, 16, 30, 30] {
        request
            .finish
            .send(Err(PostgresDDLClientError::NonRecoverableError("injected".into())))
            .unwrap();
        tokio::task::yield_now().await;
        assert_pending(admission.as_mut()).await;

        tokio::time::advance(Duration::from_secs(retry_after_secs) - Duration::from_millis(1))
            .await;
        assert!(fixture.drops.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(1)).await;
        request = fixture.next_drop().await;
        assert_eq!(request.database_name, "retry-me");
    }

    request.finish.send(Ok(())).unwrap();
    admission.await.unwrap();
    let next = fixture.next_drop().await;
    assert_eq!(next.database_name, "next");
    next.finish.send(Ok(())).unwrap();
    fixture.finish().await;
}

#[tokio::test(start_paused = true)]
async fn retry_backoff_releases_execution_capacity_for_other_drops() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        cleanup_max_pending: 2,
        cleanup_concurrency: 1,
        ..WorkerEngineConfig::default()
    });
    fixture.enqueue("retry-me").await;
    fixture
        .next_drop()
        .await
        .finish
        .send(Err(PostgresDDLClientError::OperationNotExecutedAfterCertainRetries {
            operation: "drop retry-me".into(),
            retries: 3,
        }))
        .unwrap();
    tokio::task::yield_now().await;

    fixture.enqueue("healthy").await;
    let healthy = fixture.next_drop().await;
    assert_eq!(healthy.database_name, "healthy");
    healthy.finish.send(Ok(())).unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    let retried = fixture.next_drop().await;
    assert_eq!(retried.database_name, "retry-me");
    retried.finish.send(Ok(())).unwrap();
    fixture.finish().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_cancels_active_queued_and_retrying_drops_and_blocked_admission() {
    let mut fixture = Fixture::new(WorkerEngineConfig {
        cleanup_max_pending: 3,
        cleanup_concurrency: 1,
        ..WorkerEngineConfig::default()
    });
    fixture.enqueue("retrying").await;
    fixture
        .next_drop()
        .await
        .finish
        .send(Err(PostgresDDLClientError::NonRecoverableError("injected".into())))
        .unwrap();
    tokio::task::yield_now().await;
    fixture.enqueue("active").await;
    let active = fixture.next_drop().await;
    fixture.enqueue("queued").await;
    let cleanup = fixture.cleanup.clone();
    let mut admission =
        Box::pin(cleanup.enqueue(ReadString::from("blocked"), fixture.client.clone()));
    assert_pending(admission.as_mut()).await;

    fixture.shutdown.cancel();
    fixture.finish().await;
    assert!(admission.await.is_err());
    assert!(active.finish.send(Ok(())).is_err(), "in-flight drop must be cancelled");
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(fixture.drops.try_recv().is_err(), "queued and retrying drops must not start");
    assert!(fixture.tracker.is_empty());
}

#[tokio::test]
async fn zero_cleanup_limits_are_rejected_before_connecting_to_postgres() {
    for config in [
        WorkerEngineConfig { cleanup_max_pending: 0, ..WorkerEngineConfig::default() },
        WorkerEngineConfig { cleanup_concurrency: 0, ..WorkerEngineConfig::default() },
    ] {
        assert!(WorkerEngineManager::start(PostgresConfig::default(), config).await.is_err());
    }
}
