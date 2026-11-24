//! Retry, dead-letter and routing behaviour, without a broker.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::error::{AppError, AppResult};
use crate::gateway::publisher::EventPublisher;

use super::consumer::{process_with_retry, ConsumerOptions, DeadLetter, InboundMessage};
use super::handlers::SosDispatchConsumer;
use crate::domain::Alert;
use crate::usecase::sos::SosNotifier;

fn message(payload: &str) -> InboundMessage {
    InboundMessage {
        topic: "sos.dispatched".into(),
        partition: 2,
        offset: 41,
        key: b"alert-1".to_vec(),
        payload: payload.as_bytes().to_vec(),
        headers: vec![("trace".into(), b"abc".to_vec())],
    }
}

/// Handler that fails a fixed number of times, then succeeds.
struct FlakyHandler {
    failures_left: AtomicU32,
    calls: AtomicU32,
}

#[async_trait]
impl super::consumer::MessageHandler for FlakyHandler {
    async fn handle(&self, _message: &InboundMessage) -> AppResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let remaining = self.failures_left.load(Ordering::SeqCst);
        if remaining > 0 {
            self.failures_left.fetch_sub(1, Ordering::SeqCst);
            return Err(AppError::Unavailable("gateway down".into()));
        }
        Ok(())
    }
}

type SentRecord = (String, Vec<u8>, Vec<u8>, Vec<(String, String)>);

#[derive(Default)]
struct RecordingPublisher {
    sent: Mutex<Vec<SentRecord>>,
    fail: Mutex<bool>,
}

#[async_trait]
impl EventPublisher for RecordingPublisher {
    async fn publish(
        &self,
        topic: &str,
        key: &[u8],
        payload: &[u8],
        headers: &[(String, String)],
    ) -> AppResult<()> {
        if *self.fail.lock().unwrap() {
            return Err(AppError::Unavailable("broker down".into()));
        }
        self.sent.lock().unwrap().push((
            topic.to_owned(),
            key.to_vec(),
            payload.to_vec(),
            headers.to_vec(),
        ));
        Ok(())
    }
}

#[tokio::test]
async fn success_commits_on_the_first_attempt() {
    let handler = FlakyHandler {
        failures_left: AtomicU32::new(0),
        calls: AtomicU32::new(0),
    };
    let options = ConsumerOptions {
        backoff: Duration::ZERO,
        ..ConsumerOptions::default()
    };

    let commit = process_with_retry(&handler, &message("{}"), &options, || false)
        .await
        .unwrap();
    assert!(commit);
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_flaky_handler_is_retried_then_committed() {
    let handler = FlakyHandler {
        failures_left: AtomicU32::new(2),
        calls: AtomicU32::new(0),
    };
    let options = ConsumerOptions {
        max_retries: 3,
        backoff: Duration::ZERO,
        dead_letter: None,
    };

    let commit = process_with_retry(&handler, &message("{}"), &options, || false)
        .await
        .unwrap();
    assert!(commit);
    assert_eq!(
        handler.calls.load(Ordering::SeqCst),
        3,
        "two retries then success"
    );
}

#[tokio::test]
async fn exhausted_retries_go_to_the_dead_letter_topic_with_source_headers() {
    let handler = FlakyHandler {
        failures_left: AtomicU32::new(99),
        calls: AtomicU32::new(0),
    };
    let publisher = Arc::new(RecordingPublisher::default());
    let dead_letter = Arc::new(DeadLetter::new(
        publisher.clone() as Arc<dyn EventPublisher>,
        "sos.dispatched.dlq",
    ));
    let options = ConsumerOptions {
        max_retries: 2,
        backoff: Duration::ZERO,
        dead_letter: Some(dead_letter),
    };

    let commit = process_with_retry(&handler, &message("{\"x\":1}"), &options, || false)
        .await
        .unwrap();
    assert!(commit, "a successful dead-letter write must commit");
    assert_eq!(handler.calls.load(Ordering::SeqCst), 3, "3 attempts total");

    let sent = publisher.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    let (topic, key, payload, headers) = &sent[0];
    assert_eq!(topic, "sos.dispatched.dlq");
    assert_eq!(key, b"alert-1", "partition key must be preserved");
    assert_eq!(payload, b"{\"x\":1}", "payload must travel byte-for-byte");

    let header = |name: &str| {
        headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
            .unwrap_or_default()
    };
    assert_eq!(header("trace"), "abc", "original headers are copied");
    assert_eq!(header("dlq.source.topic"), "sos.dispatched");
    assert_eq!(header("dlq.source.partition"), "2");
    assert_eq!(header("dlq.source.offset"), "41");
    assert_eq!(header("dlq.error"), "gateway down");
}

#[tokio::test]
async fn without_a_dead_letter_topic_the_message_is_dropped_after_retries() {
    let handler = FlakyHandler {
        failures_left: AtomicU32::new(99),
        calls: AtomicU32::new(0),
    };
    let options = ConsumerOptions {
        max_retries: 1,
        backoff: Duration::ZERO,
        dead_letter: None,
    };

    let commit = process_with_retry(&handler, &message("{}"), &options, || false)
        .await
        .unwrap();
    assert!(commit, "dropping still advances the offset");
    assert_eq!(handler.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_failed_dead_letter_write_leaves_the_offset_uncommitted() {
    let handler = FlakyHandler {
        failures_left: AtomicU32::new(99),
        calls: AtomicU32::new(0),
    };
    let publisher = Arc::new(RecordingPublisher::default());
    *publisher.fail.lock().unwrap() = true;
    let dead_letter = Arc::new(DeadLetter::new(
        publisher as Arc<dyn EventPublisher>,
        "sos.dispatched.dlq",
    ));
    let options = ConsumerOptions {
        max_retries: 1,
        backoff: Duration::ZERO,
        dead_letter: Some(dead_letter),
    };

    let result = process_with_retry(&handler, &message("{}"), &options, || false).await;
    assert!(result.is_err(), "the caller must stop without committing");
}

#[tokio::test]
async fn shutdown_during_the_retry_wait_does_not_commit() {
    let handler = FlakyHandler {
        failures_left: AtomicU32::new(99),
        calls: AtomicU32::new(0),
    };
    let options = ConsumerOptions {
        max_retries: 3,
        backoff: Duration::from_millis(10),
        dead_letter: None,
    };

    let commit = process_with_retry(&handler, &message("{}"), &options, || true)
        .await
        .unwrap();
    assert!(!commit, "no commit: the message must be redelivered");
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
}

#[derive(Default)]
struct CountingNotifier {
    calls: Mutex<Vec<(Vec<String>, bool)>>,
}

#[async_trait]
impl SosNotifier for CountingNotifier {
    async fn notify(&self, recipient_ids: &[String], _alert: &Alert) -> AppResult<()> {
        self.calls
            .lock()
            .unwrap()
            .push((recipient_ids.to_vec(), false));
        Ok(())
    }

    async fn notify_recipients(&self, recipient_ids: &[String], _alert: &Alert) -> AppResult<()> {
        self.calls
            .lock()
            .unwrap()
            .push((recipient_ids.to_vec(), true));
        Ok(())
    }
}

fn sos_payload(push_only: bool) -> String {
    serde_json::json!({
        "event_type": "sos.dispatched",
        "alert_id": "alert-1",
        "sender_id": "sender-1",
        "latitude": -6.2,
        "longitude": 106.8,
        "emergency_type": "MEDICAL",
        "status": "TRIGGERED",
        "recipients": ["r1", "r2"],
        "triggered_at": 1_700_000_000_000_i64,
        "severity": 0.6,
        "push_only": push_only,
    })
    .to_string()
}

#[tokio::test]
async fn sos_consumer_routes_normal_and_push_only_events() {
    let notifier = Arc::new(CountingNotifier::default());
    let consumer = SosDispatchConsumer::new(notifier.clone());

    super::consumer::MessageHandler::handle(&consumer, &message(&sos_payload(false)))
        .await
        .unwrap();
    super::consumer::MessageHandler::handle(&consumer, &message(&sos_payload(true)))
        .await
        .unwrap();

    let calls = notifier.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0, vec!["r1".to_string(), "r2".to_string()]);
    assert!(!calls[0].1, "normal dispatch uses the full fan-out");
    assert!(calls[1].1, "escalation must be push-only");
}

#[tokio::test]
async fn an_invalid_sos_payload_is_an_error_the_retry_layer_can_dead_letter() {
    let notifier = Arc::new(CountingNotifier::default());
    let consumer = SosDispatchConsumer::new(notifier.clone());
    assert!(
        super::consumer::MessageHandler::handle(&consumer, &message("not json"))
            .await
            .is_err()
    );
    assert!(notifier.calls.lock().unwrap().is_empty());
}
