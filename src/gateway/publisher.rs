//! Kafka publisher port.
//!
//! `NoopPublisher` is the default while `kafka.producer.enabled` is false: the
//! web process then notifies inline instead of handing the event to the worker,
//! which is exactly how the Go service behaves with the producer disabled.

use async_trait::async_trait;

use crate::error::AppResult;

#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publish one record. Headers are ordered pairs, not a map, so the DLQ
    /// copy can keep duplicates and ordering from the source record.
    async fn publish(
        &self,
        topic: &str,
        key: &[u8],
        payload: &[u8],
        headers: &[(String, String)],
    ) -> AppResult<()>;
}

/// Discards everything; used when Kafka is off.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopPublisher;

#[async_trait]
impl EventPublisher for NoopPublisher {
    async fn publish(
        &self,
        _topic: &str,
        _key: &[u8],
        _payload: &[u8],
        _headers: &[(String, String)],
    ) -> AppResult<()> {
        Ok(())
    }
}
