//! Kafka consumers: entity mirror topics plus the SOS fan-out.

pub mod consumer;
pub mod handlers;

#[cfg(test)]
mod tests;
