//! Outbound integrations: Kafka producer, push (FCM/APNs), SMS, storage.
//!
//! Split from `repository` because these are side effects the use cases must be
//! able to fake: the Go `TriggerSOSUseCase` took `Notifier`/`EventPublisher`
//! interfaces, and the same boundary is kept here.

pub mod kafka;
pub mod notify;
pub mod publisher;
