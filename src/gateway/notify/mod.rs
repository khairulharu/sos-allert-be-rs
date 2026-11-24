//! Push and SMS providers (`fcm_notifier.go`, `apns_notifier.go`,
//! `twilio_notifier.go`).
//!
//! Every provider takes a `base_url` so tests can point it at a local mock
//! server; production uses the vendor default. A missing configuration is
//! `NotConfigured`, which the dispatcher treats as "log only" rather than an
//! error — exactly like the Go service.

use std::time::Duration;

pub mod apns;
pub mod dispatcher;
pub mod fcm;
pub mod twilio;

pub use apns::ApnsNotifier;
pub use dispatcher::AlertDispatcher;
pub use fcm::FcmNotifier;
pub use twilio::TwilioNotifier;

/// Provider failure: not configured, or the request itself failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderError {
    NotConfigured,
    Failed(String),
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProviderError::NotConfigured => write!(f, "provider not configured"),
            ProviderError::Failed(message) => write!(f, "{message}"),
        }
    }
}

pub type ProviderResult<T> = Result<T, ProviderError>;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Shared client: a hung provider must not pin a fan-out forever.
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(DEFAULT_TIMEOUT)
        .build()
        .unwrap_or_default()
}

/// Apache's APNs/FCM error bodies are useful but must stay short.
pub async fn response_snippet(response: reqwest::Response) -> String {
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    let trimmed: String = text.chars().take(512).collect();
    format!("status {status}: {}", trimmed.trim())
}

#[cfg(test)]
mod tests;
