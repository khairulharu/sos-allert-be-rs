//! Kafka consumers: retry the same message, then dead-letter it.
//!
//! Ported from `internal/delivery/messaging/consumer.go`:
//!
//! * a handler failure retries the **same** message `max_retries` times with a
//!   fixed backoff;
//! * when the retries are exhausted the record is republished to the
//!   dead-letter topic with the original payload and key byte-for-byte plus
//!   `dlq.source.topic|partition|offset|error` headers;
//! * the offset is committed only after a success or a successful
//!   dead-letter write; a failed dead-letter write leaves the offset
//!   uncommitted so the message is redelivered;
//! * without a dead-letter topic the message is logged and committed (dropped).
//!
//! Only `sos.dispatched` gets a dead-letter publisher; the entity topics are
//! best-effort mirrors with idempotent upserts, like the Go worker.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Headers;
use rdkafka::Message;

use crate::error::{AppError, AppResult};
use crate::gateway::kafka::LoggingContext;
use crate::gateway::publisher::EventPublisher;

pub const DEFAULT_MAX_RETRIES: u32 = 3;
pub const DEFAULT_RETRY_BACKOFF: Duration = Duration::from_secs(1);
pub const DLQ_HEADER_PREFIX: &str = "dlq.";

/// One record as the handler sees it.
#[derive(Debug, Clone)]
pub struct InboundMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
}

#[async_trait]
pub trait MessageHandler: Send + Sync {
    async fn handle(&self, message: &InboundMessage) -> AppResult<()>;
}

/// Dead-letter publisher: republishes a failed record unchanged.
pub struct DeadLetter {
    publisher: Arc<dyn EventPublisher>,
    topic: String,
}

impl DeadLetter {
    pub fn new(publisher: Arc<dyn EventPublisher>, topic: impl Into<String>) -> Self {
        Self {
            publisher,
            topic: topic.into(),
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Copy the record plus the `dlq.source.*` headers. The caller treats an
    /// error as "leave the offset uncommitted".
    pub async fn publish(&self, message: &InboundMessage, cause: &AppError) -> AppResult<()> {
        let mut headers: Vec<(String, String)> = message
            .headers
            .iter()
            .map(|(name, value)| (name.clone(), String::from_utf8_lossy(value).into_owned()))
            .collect();
        headers.push((
            format!("{DLQ_HEADER_PREFIX}source.topic"),
            message.topic.clone(),
        ));
        headers.push((
            format!("{DLQ_HEADER_PREFIX}source.partition"),
            message.partition.to_string(),
        ));
        headers.push((
            format!("{DLQ_HEADER_PREFIX}source.offset"),
            message.offset.to_string(),
        ));
        headers.push((format!("{DLQ_HEADER_PREFIX}error"), cause.to_string()));

        self.publisher
            .publish(&self.topic, &message.key, &message.payload, &headers)
            .await
    }
}

#[derive(Clone)]
pub struct ConsumerOptions {
    /// Retries after the first attempt (`0` uses the default; use
    /// `max_retries_override` for none).
    pub max_retries: u32,
    pub backoff: Duration,
    /// `None` means "dropped after the retries", like the entity topics.
    pub dead_letter: Option<Arc<DeadLetter>>,
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            backoff: DEFAULT_RETRY_BACKOFF,
            dead_letter: None,
        }
    }
}

impl ConsumerOptions {
    pub fn attempts(&self) -> u32 {
        self.max_retries.saturating_add(1)
    }
}

/// Run the handler with retries. `Ok(true)` means "commit this offset";
/// `Ok(false)` means the run is shutting down mid-retry (no commit, the
/// message is redelivered); `Err` means the dead-letter write failed and the
/// consumer must stop without committing.
pub async fn process_with_retry(
    handler: &dyn MessageHandler,
    message: &InboundMessage,
    options: &ConsumerOptions,
    cancelled: impl Fn() -> bool,
) -> AppResult<bool> {
    let attempts = options.attempts();
    let mut last_error: Option<AppError> = None;

    for attempt in 1..=attempts {
        if attempt > 1 {
            if cancelled() {
                tracing::warn!(
                    topic = %message.topic,
                    offset = message.offset,
                    "consumer stopping before the retry wait finished; leaving offset uncommitted"
                );
                return Ok(false);
            }
            if !options.backoff.is_zero() {
                tokio::time::sleep(options.backoff).await;
            }
            if cancelled() {
                return Ok(false);
            }
            tracing::warn!(
                topic = %message.topic,
                offset = message.offset,
                attempt,
                max_attempts = attempts,
                error = %last_error.as_ref().map(ToString::to_string).unwrap_or_default(),
                "retrying message"
            );
        }

        match handler.handle(message).await {
            Ok(()) => return Ok(true),
            Err(error) => {
                tracing::error!(
                    topic = %message.topic,
                    offset = message.offset,
                    attempt,
                    max_attempts = attempts,
                    error = %error,
                    "failed to process message"
                );
                last_error = Some(error);
            }
        }
    }

    let Some(dead_letter) = options.dead_letter.as_ref() else {
        tracing::error!(
            topic = %message.topic,
            offset = message.offset,
            "no dead-letter topic configured; dropping message after its retries"
        );
        return Ok(true);
    };

    let cause = last_error.unwrap_or_else(|| AppError::Internal(anyhow::anyhow!("unknown error")));
    match dead_letter.publish(message, &cause).await {
        Ok(()) => {
            tracing::warn!(
                topic = %message.topic,
                offset = message.offset,
                dlq_topic = dead_letter.topic(),
                "message moved to the dead-letter topic"
            );
            Ok(true)
        }
        Err(error) => {
            tracing::error!(
                topic = %message.topic,
                offset = message.offset,
                error = %error,
                "failed to publish to the dead-letter topic; leaving offset uncommitted"
            );
            Err(error)
        }
    }
}

pub struct ConsumerSettings {
    pub brokers: String,
    pub group: String,
    /// `earliest` or `latest` (`kafka.auto.offset.reset`).
    pub auto_offset_reset: String,
}

/// One topic consumer. Returns when `shutdown` resolves or when a dead-letter
/// write failed (the caller restarts it, which replays from the last
/// committed offset).
pub async fn run_consumer(
    settings: ConsumerSettings,
    topic: &str,
    handler: Arc<dyn MessageHandler>,
    options: ConsumerOptions,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> AppResult<()> {
    let consumer: StreamConsumer<LoggingContext> = ClientConfig::new()
        .set("bootstrap.servers", &settings.brokers)
        .set("group.id", &settings.group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", &settings.auto_offset_reset)
        // A slow handler must not be mistaken for a dead member.
        .set("max.poll.interval.ms", "300000")
        .create_with_context(LoggingContext)
        .map_err(|e| AppError::Unavailable(format!("kafka consumer {topic}: {e}")))?;

    consumer
        .subscribe(&[topic])
        .map_err(|e| AppError::Unavailable(format!("kafka subscribe {topic}: {e}")))?;

    tracing::info!(topic, group = %settings.group, "consumer started");

    loop {
        let received = tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            received = consumer.recv() => received,
        };

        let borrowed = match received {
            Ok(message) => message,
            Err(e) => {
                tracing::warn!(topic, error = %e, "receive failed");
                continue;
            }
        };

        let message = InboundMessage {
            topic: borrowed.topic().to_owned(),
            partition: borrowed.partition(),
            offset: borrowed.offset(),
            key: borrowed.key().map(|k| k.to_vec()).unwrap_or_default(),
            payload: borrowed.payload().map(|p| p.to_vec()).unwrap_or_default(),
            headers: borrowed
                .headers()
                .map(|headers| {
                    (0..headers.count())
                        .map(|index| {
                            let header = headers.get(index);
                            (
                                header.key.to_owned(),
                                header.value.map(|value| value.to_vec()).unwrap_or_default(),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
        };

        let cancelled = {
            let shutdown = &shutdown;
            move || *shutdown.borrow()
        };

        match process_with_retry(handler.as_ref(), &message, &options, cancelled).await {
            Ok(true) => {
                // Commit only after the message is handled or dead-lettered.
                if let Err(e) = consumer.commit_message(&borrowed, CommitMode::Async) {
                    tracing::warn!(topic, error = %e, "offset commit failed");
                }
            }
            Ok(false) => return Ok(()),
            Err(e) => {
                // The dead-letter write failed: stop so the group replays from
                // the last committed offset instead of committing past it.
                return Err(e);
            }
        }
    }
}
