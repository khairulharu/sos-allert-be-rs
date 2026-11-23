//! Kafka consumers (`users`, `contacts`, `addresses`, `sos.dispatched`).
//!
//! Phase 5 ports the Go retry/DLQ semantics:
//! * retry the SAME message `kafka.consumer.max_retries` times, then publish to
//!   `kafka.dlq.topic` with the original payload/key byte-for-byte plus
//!   `dlq.source.*` headers, then mark;
//! * a failed DLQ publish must leave the offset uncommitted;
//! * `sos.dispatched` keeps the legacy group id, the fan-out topics use one
//!   group per topic so a rebalance cannot drag the others.
//!
//! Client: `rskafka` (pure Rust, no librdkafka/cmake) unless a measured reason
//! appears to take on the C client.
