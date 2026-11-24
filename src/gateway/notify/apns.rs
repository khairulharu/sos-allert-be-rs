//! Apple Push Notification service, ES256 provider token + critical alerts
//! (`apns_notifier.go`).
//!
//! The provider token is cached for 50 minutes. Apple allows one token update
//! per 20 minutes and answers `403 TooManyProviderTokenUpdates` when a key
//! mints several in a short window — which is what happened when the Go client
//! built a JWT per device in one fan-out.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::json;

use crate::config::ApnsConfig;

use super::{http_client, response_snippet, ProviderError, ProviderResult};

pub const PRODUCTION_URL: &str = "https://api.push.apple.com";
pub const SANDBOX_URL: &str = "https://api.sandbox.push.apple.com";
/// Refresh inside the 1 hour `exp` with margin for clock skew.
pub const PROVIDER_TOKEN_TTL: Duration = Duration::from_secs(50 * 60);

pub struct ApnsNotifier {
    enabled: bool,
    key_id: String,
    team_id: String,
    bundle_id: String,
    private_key_pem: Vec<u8>,
    base_url: String,
    client: reqwest::Client,
    cache: Mutex<Option<(String, Instant)>>,
}

impl ApnsNotifier {
    pub fn new(config: &ApnsConfig, private_key_pem: Vec<u8>) -> Self {
        Self {
            enabled: config.enabled,
            key_id: config.key_id.clone(),
            team_id: config.team_id.clone(),
            bundle_id: config.bundle_id.clone(),
            private_key_pem,
            base_url: if config.production {
                PRODUCTION_URL.to_string()
            } else {
                SANDBOX_URL.to_string()
            },
            client: http_client(),
            cache: Mutex::new(None),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into().trim_end_matches('/').to_string();
        self
    }

    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    pub fn configured(&self) -> bool {
        self.enabled
            && !self.key_id.is_empty()
            && !self.team_id.is_empty()
            && !self.bundle_id.is_empty()
            && !self.private_key_pem.is_empty()
    }

    /// ES256 provider token: `{alg: ES256, kid}` / `{iss, iat, exp}`.
    pub fn build_jwt(&self, now: Instant) -> ProviderResult<String> {
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

        // `Instant` has no wall clock; the claims only need a consistent pair.
        let issued = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0) as i64;
        let _ = now;

        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.key_id.clone());

        let claims = json!({
            "iss": self.team_id,
            "iat": issued,
            "exp": issued + 3600,
        });

        let key = EncodingKey::from_ec_pem(&self.private_key_pem)
            .map_err(|e| ProviderError::Failed(format!("apns key: {e}")))?;
        encode(&header, &claims, &key).map_err(|e| ProviderError::Failed(format!("apns jwt: {e}")))
    }

    /// Cached provider token; minted again only after [`PROVIDER_TOKEN_TTL`].
    pub fn provider_token(&self) -> ProviderResult<String> {
        let now = Instant::now();
        let mut cache = self.cache.lock().expect("apns cache poisoned");
        if let Some((token, minted)) = cache.as_ref() {
            if now.duration_since(*minted) < PROVIDER_TOKEN_TTL {
                return Ok(token.clone());
            }
        }
        let token = self.build_jwt(now)?;
        *cache = Some((token.clone(), now));
        Ok(token)
    }

    /// Critical alert to one device token.
    pub async fn send(
        &self,
        device_token: &str,
        title: &str,
        body: &str,
        alert_id: &str,
    ) -> ProviderResult<()> {
        if !self.configured() || device_token.is_empty() {
            return Err(ProviderError::NotConfigured);
        }

        let token = self.provider_token()?;
        let payload = json!({
            "aps": {
                "alert": { "title": title, "body": body },
                "sound": { "critical": 1, "name": "siren.wav", "volume": 1.0 },
                "badge": 1,
                "interruption-level": "critical",
                "category": "SOS_ALERT"
            },
            "alert_id": alert_id
        });

        let url = format!("{}/3/device/{}", self.base_url, device_token);
        let response = self
            .client
            .post(&url)
            .bearer_auth(token)
            .header("apns-topic", &self.bundle_id)
            .header("apns-push-type", "alert")
            .header("apns-priority", "10")
            .header("apns-expiration", "0")
            .json(&payload)
            .send()
            .await
            .map_err(|e| ProviderError::Failed(format!("apns request: {e}")))?;

        if response.status().is_success() {
            return Ok(());
        }
        Err(ProviderError::Failed(format!(
            "apns request failed: {}",
            response_snippet(response).await
        )))
    }
}
