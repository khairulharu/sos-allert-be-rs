//! Firebase Cloud Messaging HTTP v1 (`fcm_notifier.go`).

use std::collections::HashMap;

use serde_json::json;

use crate::config::FcmConfig;

use super::{http_client, response_snippet, ProviderError, ProviderResult};

pub const DEFAULT_BASE_URL: &str = "https://fcm.googleapis.com";

pub struct FcmNotifier {
    enabled: bool,
    project_id: String,
    access_token: String,
    base_url: String,
    client: reqwest::Client,
}

impl FcmNotifier {
    pub fn new(config: &FcmConfig) -> Self {
        Self {
            enabled: config.enabled,
            project_id: config.project_id.clone(),
            access_token: config.access_token.clone(),
            base_url: DEFAULT_BASE_URL.to_string(),
            client: http_client(),
        }
    }

    /// Test hook: point the provider at a local mock server.
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into().trim_end_matches('/').to_string();
        self
    }

    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    /// Per-user topic: clients subscribe to `user_<id>` on login, so no device
    /// token has to be stored.
    pub async fn send_to_topic(
        &self,
        topic: &str,
        title: &str,
        body: &str,
        data: &HashMap<String, String>,
    ) -> ProviderResult<()> {
        self.send(self.payload(Some(topic), None, title, body, data))
            .await
    }

    pub async fn send_to_token(
        &self,
        token: &str,
        title: &str,
        body: &str,
        data: &HashMap<String, String>,
    ) -> ProviderResult<()> {
        self.send(self.payload(None, Some(token), title, body, data))
            .await
    }

    /// Payload shape is copied field for field from the Go notifier: Android
    /// gets the `sos_alerts` channel and a siren, iOS an APNs critical alert.
    fn payload(
        &self,
        topic: Option<&str>,
        token: Option<&str>,
        title: &str,
        body: &str,
        data: &HashMap<String, String>,
    ) -> serde_json::Value {
        let mut message = json!({
            "notification": { "title": title, "body": body },
            "data": data,
            "android": {
                "priority": "HIGH",
                "notification": {
                    "sound": "siren",
                    "channel_id": "sos_alerts",
                    "default_sound": true
                }
            },
            "apns": {
                "payload": {
                    "aps": {
                        "alert": { "title": title, "body": body },
                        "sound": { "critical": 1, "name": "siren.wav", "volume": 1.0 },
                        "interruption-level": "critical"
                    }
                }
            }
        });
        if let Some(topic) = topic {
            message["topic"] = json!(topic);
        }
        if let Some(token) = token {
            message["token"] = json!(token);
        }
        json!({ "message": message })
    }

    async fn send(&self, payload: serde_json::Value) -> ProviderResult<()> {
        if !self.enabled || self.project_id.is_empty() || self.access_token.is_empty() {
            return Err(ProviderError::NotConfigured);
        }

        let url = format!(
            "{}/v1/projects/{}/messages:send",
            self.base_url, self.project_id
        );
        let response = self
            .client
            .post(&url)
            .bearer_auth(&self.access_token)
            .json(&payload)
            .send()
            .await
            .map_err(|e| ProviderError::Failed(format!("fcm request: {e}")))?;

        if response.status().is_success() {
            return Ok(());
        }
        Err(ProviderError::Failed(format!(
            "fcm request failed: {}",
            response_snippet(response).await
        )))
    }
}
