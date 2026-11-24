//! Kafka transport (`rdkafka` / librdkafka).
//!
//! Chosen over a pure-Rust client because the Go service relies on **consumer
//! groups**: one group per fan-out topic, a legacy group id for
//! `sos.dispatched`, and commits that only happen after a message is handled
//! or dead-lettered. `rdkafka` provides exactly that; `rskafka` has no group
//! membership.

use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientContext;

use crate::error::{AppError, AppResult};
use crate::gateway::publisher::EventPublisher;

/// Producer that publishes and waits for the broker acknowledgement.
pub struct KafkaPublisher {
    producer: FutureProducer,
    /// Delivery timeout for one record.
    timeout: Duration,
}

impl KafkaPublisher {
    /// Build a producer without contacting the broker; the first `send` does.
    pub fn new(brokers: &str, timeout: Duration) -> AppResult<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", timeout.as_millis().to_string())
            .set("client.id", "sos-allert-be-rs")
            .create()
            .map_err(|e| AppError::Unavailable(format!("kafka producer: {e}")))?;
        Ok(Self { producer, timeout })
    }

    pub async fn send(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        payload: Vec<u8>,
        headers: Vec<(String, Vec<u8>)>,
    ) -> AppResult<()> {
        let mut record: FutureRecord<'_, Vec<u8>, Vec<u8>> =
            FutureRecord::to(topic).payload(&payload);
        let key_bytes = key.map(|k| k.to_vec());
        if let Some(key_ref) = key_bytes.as_ref() {
            record = record.key(key_ref);
        }

        let mut owned_headers = rdkafka::message::OwnedHeaders::new();
        for (name, value) in &headers {
            owned_headers = owned_headers.insert(rdkafka::message::Header {
                key: name,
                value: Some(value.as_slice()),
            });
        }
        record = record.headers(owned_headers);

        self.producer
            .send(record, Timeout::After(self.timeout))
            .await
            .map(|_| ())
            .map_err(|(e, _)| AppError::Unavailable(format!("kafka produce {topic}: {e}")))
    }
}

#[async_trait]
impl EventPublisher for KafkaPublisher {
    async fn publish(
        &self,
        topic: &str,
        key: &[u8],
        payload: &[u8],
        headers: &[(String, String)],
    ) -> AppResult<()> {
        let headers: Vec<(String, Vec<u8>)> = headers
            .iter()
            .map(|(name, value)| (name.clone(), value.as_bytes().to_vec()))
            .collect();
        self.send(topic, Some(key), payload.to_vec(), headers).await
    }
}

/// Client context that logs librdkafka's own errors instead of swallowing them.
#[derive(Clone, Debug, Default)]
pub struct LoggingContext;

impl ClientContext for LoggingContext {
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        tracing::error!(error = %error, reason, "librdkafka error");
    }
}

impl rdkafka::consumer::ConsumerContext for LoggingContext {}
