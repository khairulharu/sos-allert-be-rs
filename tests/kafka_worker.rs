//! End-to-end Kafka: the real broker, the real consumer loop, the real
//! dead-letter path.
//!
//! Run with:
//! `TEST_KAFKA_BROKERS=127.0.0.1:9092 cargo test --test kafka_worker -- --nocapture`
//! Skipped (passes with a note) when the variable is unset.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use rdkafka::producer::{FutureProducer, FutureRecord};
use sos_allert::delivery::messaging::consumer::{
    run_consumer, ConsumerOptions, ConsumerSettings, DeadLetter,
};
use sos_allert::delivery::messaging::handlers::SosDispatchConsumer;
use sos_allert::error::AppResult;
use sos_allert::gateway::kafka::KafkaPublisher;
use sos_allert::gateway::publisher::EventPublisher;
use sos_allert::model::events::topics;
use sos_allert::model::sos::AlertNotice;
use sos_allert::usecase::sos::SosNotifier;
use tokio::sync::mpsc;

struct ChannelNotifier {
    sender: mpsc::UnboundedSender<(Vec<String>, bool)>,
}

#[async_trait]
impl SosNotifier for ChannelNotifier {
    async fn notify(&self, recipient_ids: &[String], _notice: &AlertNotice) -> AppResult<()> {
        let _ = self.sender.send((recipient_ids.to_vec(), false));
        Ok(())
    }

    async fn notify_recipients(
        &self,
        recipient_ids: &[String],
        _notice: &AlertNotice,
    ) -> AppResult<()> {
        let _ = self.sender.send((recipient_ids.to_vec(), true));
        Ok(())
    }
}

fn brokers() -> Option<String> {
    match std::env::var("TEST_KAFKA_BROKERS") {
        Ok(value) if !value.is_empty() => Some(value),
        _ => {
            eprintln!("skip: TEST_KAFKA_BROKERS not set");
            None
        }
    }
}

async fn producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("producer")
}

async fn produce(brokers: &str, topic: &str, key: &str, payload: &str) {
    let producer = producer(brokers).await;
    let record: FutureRecord<'_, str, str> = FutureRecord::to(topic).payload(payload).key(key);
    producer
        .send(record, Duration::from_secs(10))
        .await
        .expect("produce");
}

fn sos_event(alert_id: &str, push_only: bool) -> String {
    serde_json::json!({
        "event_type": "sos.dispatched",
        "alert_id": alert_id,
        "sender_id": "sender-1",
        "latitude": -6.2,
        "longitude": 106.8,
        "emergency_type": "MEDICAL",
        "status": "TRIGGERED",
        "recipients": ["r1", "r2", "r3"],
        "triggered_at": 1_700_000_000_000_i64,
        "severity": 0.6,
        "push_only": push_only,
    })
    .to_string()
}

#[tokio::test]
async fn consumer_receives_events_and_dead_letters_poison_messages() {
    let Some(brokers) = brokers() else { return };

    let unique = uuid::Uuid::new_v4().to_string();
    let group = format!("rs-e2e-{}", &unique[..8]);
    let dlq_topic = format!("sos.dispatched.dlq.{}", &unique[..8]);

    let (tx, mut rx) = mpsc::unbounded_channel();
    let handler = Arc::new(
        SosDispatchConsumer::new(Arc::new(ChannelNotifier { sender: tx }))
            .with_timeout(Duration::from_secs(5)),
    );

    let dlq_publisher = KafkaPublisher::new(&brokers, Duration::from_secs(10)).expect("producer");
    let dead_letter = Arc::new(DeadLetter::new(
        Arc::new(dlq_publisher) as Arc<dyn EventPublisher>,
        dlq_topic.clone(),
    ));

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(run_consumer(
        ConsumerSettings {
            brokers: brokers.clone(),
            group,
            auto_offset_reset: "earliest".into(),
        },
        topics::SOS_DISPATCHED,
        handler,
        ConsumerOptions {
            max_retries: 1,
            backoff: Duration::from_millis(50),
            dead_letter: Some(dead_letter),
        },
        shutdown_rx,
    ));

    // Let the group join before producing.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let alert_id = format!("alert-{}", &unique[..8]);
    produce(
        &brokers,
        topics::SOS_DISPATCHED,
        &alert_id,
        &sos_event(&alert_id, false),
    )
    .await;

    let delivered = tokio::time::timeout(Duration::from_secs(20), rx.recv())
        .await
        .expect("no notification within 20 s")
        .expect("channel closed");
    assert_eq!(delivered.0, vec!["r1", "r2", "r3"]);
    assert!(!delivered.1, "a normal dispatch is not push-only");

    // A push-only (escalation) event must use the recipient-only surface.
    let escalation_id = format!("esc-{}", &unique[..8]);
    produce(
        &brokers,
        topics::SOS_DISPATCHED,
        &escalation_id,
        &sos_event(&escalation_id, true),
    )
    .await;
    let escalation = tokio::time::timeout(Duration::from_secs(20), rx.recv())
        .await
        .expect("no escalation within 20 s")
        .expect("channel closed");
    assert!(escalation.1, "escalation must be push-only");

    // A poison payload exhausts its retries and lands on the DLQ.
    produce(&brokers, topics::SOS_DISPATCHED, "poison", "not json").await;

    let dlq_consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set(
            "group.id",
            format!("rs-e2e-dlq-{}", &uuid::Uuid::new_v4().to_string()[..8]),
        )
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .create()
        .expect("dlq consumer");
    dlq_consumer
        .subscribe(&[&dlq_topic])
        .expect("subscribe dlq");

    let mut found = false;
    for _ in 0..40 {
        let received = tokio::time::timeout(Duration::from_secs(2), dlq_consumer.recv()).await;
        if let Ok(Ok(message)) = received {
            {
                if let Some(payload) = message.payload() {
                    if payload == b"not json" {
                        assert_eq!(message.key(), Some(&b"poison"[..]));
                        let headers = message.headers().expect("headers");
                        let mut seen = Vec::new();
                        for index in 0..headers.count() {
                            let header = headers.get(index);
                            seen.push((
                                header.key.to_owned(),
                                String::from_utf8_lossy(header.value.unwrap_or_default())
                                    .into_owned(),
                            ));
                        }
                        assert!(seen.iter().any(|(k, _)| k == "dlq.source.topic"));
                        assert!(seen
                            .iter()
                            .any(|(k, v)| k == "dlq.source.partition" && v == "0"));
                        assert!(seen.iter().any(|(k, _)| k == "dlq.error"));
                        found = true;
                    }
                }
            }
        }
        if found {
            break;
        }
    }
    assert!(
        found,
        "the poison message never reached the dead-letter topic"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
}
