use pgtest_utils::read_string::ReadString;

use super::*;
use crate::worker_engine::traits::ConsumerIO;

#[tokio::test]
async fn dropping_an_unread_attach_reply_detaches_the_registered_generation() {
    let (engine_tx, mut engine_rx) = hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let consumer = ConsumerWorker {
        oneshot_channel: reply_tx,
        attachment: Some((LeaseId::from("test"), engine_tx)),
    };

    assert!(
        consumer
            .reply(ConsumerReply::Attached {
                database_name: ReadString::from("clone"),
                generation: 42,
                cancellation: CancellationToken::new(),
            })
            .is_ok()
    );

    drop(reply_rx);
    assert!(matches!(engine_rx.try_recv(),
        Ok(EngineMessage::Detach { lease, generation: 42 }) if lease.as_ref() == "test"));
}

#[tokio::test]
async fn failed_attach_delivery_leaves_rollback_to_the_engine() {
    let (engine_tx, mut engine_rx) = hotpath::channel!(tokio::sync::mpsc::unbounded_channel());
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let consumer = ConsumerWorker {
        oneshot_channel: reply_tx,
        attachment: Some((LeaseId::from("test"), engine_tx)),
    };
    drop(reply_rx);
    assert!(
        consumer
            .reply(ConsumerReply::Attached {
                database_name: ReadString::from("clone"),
                generation: 42,
                cancellation: CancellationToken::new(),
            })
            .is_err()
    );
    assert!(engine_rx.try_recv().is_err(), "a failed send must not also enqueue Detach");
}
