//! VAPID Web Push (`webpush_notifier.go`, RFC 8291/8292).
//!
//! Encryption and the VAPID JWT come from the `web-push` crate; the HTTP send
//! goes through a small [`PushTransport`] so tests can assert the endpoint,
//! headers and ciphertext without TLS or a live push service.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use web_push::{
    ContentEncoding, SubscriptionInfo, Urgency, VapidSignatureBuilder, WebPushMessageBuilder,
};

use crate::config::WebPushConfig;
use crate::error::AppResult;
use crate::usecase::push::{hosts_or, is_allowed_endpoint, PushSubscriptionStore};

use super::{http_client, ProviderError, ProviderResult};

/// What a service worker's `push` handler receives.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WebPushPayload {
    pub title: String,
    pub body: String,
    pub data: HashMap<String, String>,
}

/// One HTTP POST to a push service. Swappable for tests.
#[async_trait]
pub trait PushTransport: Send + Sync {
    /// Returns the HTTP status; an error means the request never completed.
    async fn send(
        &self,
        endpoint: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<u16, String>;
}

pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl Default for ReqwestTransport {
    fn default() -> Self {
        Self {
            client: http_client(),
        }
    }
}

impl ReqwestTransport {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl PushTransport for ReqwestTransport {
    async fn send(
        &self,
        endpoint: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<u16, String> {
        let mut request = self.client.post(endpoint).body(body);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request
            .send()
            .await
            .map_err(|e| format!("web push request: {e}"))?;
        Ok(response.status().as_u16())
    }
}

pub struct WebPushNotifier {
    enabled: bool,
    vapid_public_key: String,
    vapid_private_key: String,
    subscriber: String,
    ttl_seconds: u32,
    allowed_hosts: Vec<String>,
    subs: Arc<dyn PushSubscriptionStore>,
    transport: Arc<dyn PushTransport>,
}

impl WebPushNotifier {
    pub fn new(config: &WebPushConfig, subs: Arc<dyn PushSubscriptionStore>) -> Self {
        Self {
            enabled: config.enabled,
            vapid_public_key: config.vapid_public_key.clone(),
            vapid_private_key: config.vapid_private_key.clone(),
            subscriber: config.subscriber.clone(),
            ttl_seconds: config.ttl_seconds,
            allowed_hosts: hosts_or(&config.allowed_hosts),
            subs,
            transport: Arc::new(ReqwestTransport::default()),
        }
    }

    pub fn with_transport(mut self, transport: Arc<dyn PushTransport>) -> Self {
        self.transport = transport;
        self
    }

    pub fn configured(&self) -> bool {
        self.enabled && !self.vapid_public_key.is_empty() && !self.vapid_private_key.is_empty()
    }

    /// JSON a service worker reads; `url` points the app at the inbox.
    pub fn build_payload(
        title: &str,
        body: &str,
        data: &HashMap<String, String>,
    ) -> ProviderResult<Vec<u8>> {
        let mut merged = HashMap::with_capacity(data.len() + 1);
        merged.insert("url".to_string(), "/".to_string());
        for (key, value) in data {
            merged.insert(key.clone(), value.clone());
        }
        serde_json::to_vec(&WebPushPayload {
            title: title.to_owned(),
            body: body.to_owned(),
            data: merged,
        })
        .map_err(|e| ProviderError::Failed(format!("web push payload: {e}")))
    }

    /// Push to every subscription the user registered. A subscription the
    /// service reports as gone (404/410) and one whose host is no longer
    /// allowlisted are deleted; other failures surface as the first error.
    pub async fn send_to_user(
        &self,
        user_id: &str,
        title: &str,
        body: &str,
        data: &HashMap<String, String>,
    ) -> ProviderResult<()> {
        if !self.configured() {
            return Err(ProviderError::NotConfigured);
        }

        let subs = self
            .subs
            .list_by_user(user_id)
            .await
            .map_err(|e| ProviderError::Failed(format!("list push subscriptions: {e}")))?;
        if subs.is_empty() {
            tracing::info!(user_id, "no browser push subscription");
            return Ok(());
        }

        let payload = Self::build_payload(title, body, data)?;
        let mut first_error: Option<ProviderError> = None;

        for sub in subs {
            // Re-check at send time: a row written before the allowlist
            // changed must never become an outbound target.
            if !is_allowed_endpoint(&sub.endpoint, &self.allowed_hosts) {
                tracing::warn!(
                    user_id,
                    "dropping push subscription with a non-allowlisted endpoint"
                );
                let _ = self.subs.delete_by_endpoint(&sub.endpoint).await;
                continue;
            }

            match self
                .send_one(&sub.endpoint, &sub.p256dh, &sub.auth, &payload)
                .await
            {
                Ok(status) if status == 404 || status == 410 => {
                    tracing::info!(user_id, "push endpoint gone, deleting subscription");
                    if let Err(e) = self.subs.delete_by_endpoint(&sub.endpoint).await {
                        tracing::warn!(error = %e, "could not delete stale push subscription");
                    }
                }
                Ok(status) if status >= 400 => {
                    tracing::warn!(user_id, status, "web push send failed");
                    if first_error.is_none() {
                        first_error = Some(ProviderError::Failed(format!(
                            "push service returned {status}"
                        )));
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(user_id, error = %e, "web push send failed");
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn send_one(
        &self,
        endpoint: &str,
        p256dh: &str,
        auth: &str,
        payload: &[u8],
    ) -> Result<u16, ProviderError> {
        let info = SubscriptionInfo::new(endpoint, p256dh, auth);

        let mut builder = WebPushMessageBuilder::new(&info);
        builder.set_ttl(self.ttl_seconds);
        builder.set_urgency(Urgency::High);
        builder.set_payload(ContentEncoding::Aes128Gcm, payload);

        let signature = VapidSignatureBuilder::from_base64_no_sub(&self.vapid_private_key)
            .map_err(|e| ProviderError::Failed(format!("vapid key: {e}")))?
            .add_sub_info(&info)
            .build()
            .map_err(|e| ProviderError::Failed(format!("vapid signature: {e}")))?;
        builder.set_vapid_signature(signature);

        let message = builder
            .build()
            .map_err(|e| ProviderError::Failed(format!("web push build: {e}")))?;

        let Some(payload) = message.payload else {
            return Err(ProviderError::Failed("web push payload missing".into()));
        };

        let mut headers: Vec<(String, String)> = vec![
            ("TTL".into(), message.ttl.to_string()),
            ("Urgency".into(), "high".into()),
            ("Content-Type".into(), "application/octet-stream".into()),
        ];
        if let Some(content_encoding) = Some(payload.content_encoding) {
            headers.push((
                "Content-Encoding".into(),
                match content_encoding {
                    ContentEncoding::Aes128Gcm => "aes128gcm",
                    ContentEncoding::AesGcm => "aesgcm",
                }
                .to_string(),
            ));
        }
        for (name, value) in payload.crypto_headers {
            headers.push((name.to_string(), value));
        }

        self.transport
            .send(&message.endpoint.to_string(), headers, payload.content)
            .await
            .map_err(ProviderError::Failed)
    }

    /// Contact for the VAPID `sub` claim (`webpush.subscriber`).
    pub fn subscriber(&self) -> &str {
        &self.subscriber
    }
}

impl std::fmt::Debug for WebPushNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebPushNotifier")
            .field("enabled", &self.enabled)
            .field("ttl_seconds", &self.ttl_seconds)
            .finish_non_exhaustive()
    }
}

/// Keeps the trait object usable from the dispatcher.
pub type SharedWebPush = Arc<WebPushNotifier>;

#[allow(dead_code)]
fn _app_result(_r: AppResult<()>) {}

#[cfg(test)]
mod tests;
