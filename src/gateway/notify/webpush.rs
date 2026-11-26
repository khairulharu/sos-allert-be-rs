//! VAPID Web Push (`webpush_notifier.go`, RFC 8291/8292).
//!
//! The message encryption and the VAPID JWT are implemented here with `p256` +
//! `aes-gcm` + `hkdf` instead of the `web-push` crate: that crate pulls
//! `jwt-simple` → `p256 0.13` → `crypto-bigint 0.5`, which carries a
//! timing-variability advisory on exactly the signing path we use.
//!
//! The HTTP send goes through a small [`PushTransport`] so tests can assert the
//! endpoint, headers and ciphertext without TLS or a live push service.

use std::collections::HashMap;
use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::Aes128Gcm;
use base64::Engine;
use hkdf::Hkdf;
use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use p256::elliptic_curve::sec1::Sec1Point;
use p256::{PublicKey, SecretKey};
use sha2::Sha256;

use crate::config::WebPushConfig;
use crate::error::AppResult;
use crate::usecase::push::{hosts_or, is_allowed_endpoint, PushSubscriptionStore};

use super::{http_client, ProviderError, ProviderResult};

/// VAPID tokens are valid for 12 hours (the RFC caps them at 24).
pub const VAPID_TTL_SECONDS: i64 = 12 * 3600;
/// RFC 8188 record size for the single record we send.
const RECORD_SIZE: u32 = 4096;
/// Uncompressed P-256 point length.
const UNCOMPRESSED_POINT_LEN: usize = 65;

/// What a service worker's `push` handler receives.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WebPushPayload {
    pub title: String,
    pub body: String,
    pub data: HashMap<String, String>,
}

/// One HTTP POST to a push service. Swappable for tests.
#[async_trait::async_trait]
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

#[async_trait::async_trait]
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
        let body = self.encrypt(p256dh, auth, payload)?;
        let authorization = self.vapid_authorization(endpoint)?;

        let headers = vec![
            ("TTL".to_string(), self.ttl_seconds.to_string()),
            ("Urgency".to_string(), "high".to_string()),
            (
                "Content-Type".to_string(),
                "application/octet-stream".to_string(),
            ),
            ("Content-Encoding".to_string(), "aes128gcm".to_string()),
            ("Authorization".to_string(), authorization),
        ];

        self.transport
            .send(endpoint, headers, body)
            .await
            .map_err(ProviderError::Failed)
    }

    /// RFC 8188 `aes128gcm` body for one record:
    /// `salt(16) | rs(4) | idlen(1) | keyid(as_public 65) | ciphertext`.
    pub fn encrypt(
        &self,
        p256dh: &str,
        auth: &str,
        payload: &[u8],
    ) -> Result<Vec<u8>, ProviderError> {
        let ua_public_bytes = decode_b64url(p256dh)?;
        let ua_public = PublicKey::from_sec1_bytes(&ua_public_bytes)
            .map_err(|e| ProviderError::Failed(format!("subscription key: {e}")))?;
        let auth_secret = decode_b64url(auth)?;

        let as_secret = random_secret()?;
        let as_public = uncompressed(&as_secret.public_key());

        let shared =
            p256::ecdh::diffie_hellman(as_secret.to_nonzero_scalar(), ua_public.as_affine());

        // RFC 8291 §3.4: ikm = HKDF(auth_secret, ecdh, "WebPush: info\0" || ua || as)
        let mut auth_info = Vec::with_capacity(14 + ua_public_bytes.len() + as_public.len());
        auth_info.extend_from_slice(b"WebPush: info\0");
        auth_info.extend_from_slice(&ua_public_bytes);
        auth_info.extend_from_slice(&as_public);

        let auth_hkdf = Hkdf::<Sha256>::new(Some(&auth_secret), shared.raw_secret_bytes());
        let mut ikm = [0_u8; 32];
        auth_hkdf
            .expand(&auth_info, &mut ikm)
            .map_err(|_| ProviderError::Failed("web push ikm".into()))?;

        let salt = random_array::<16>()?;
        let hkdf = Hkdf::<Sha256>::new(Some(&salt), &ikm);
        let mut cek = [0_u8; 16];
        hkdf.expand(b"Content-Encoding: aes128gcm\0", &mut cek)
            .map_err(|_| ProviderError::Failed("web push cek".into()))?;
        let mut nonce = [0_u8; 12];
        hkdf.expand(b"Content-Encoding: nonce\0", &mut nonce)
            .map_err(|_| ProviderError::Failed("web push nonce".into()))?;

        let cipher = Aes128Gcm::new_from_slice(&cek)
            .map_err(|_| ProviderError::Failed("web push cipher".into()))?;
        let mut plaintext = payload.to_vec();
        // 0x02 marks the last (only) record.
        plaintext.push(0x02);
        let nonce_ref = aes_gcm::Nonce::from(nonce);
        let ciphertext = cipher
            .encrypt(&nonce_ref, plaintext.as_ref())
            .map_err(|_| ProviderError::Failed("web push encrypt".into()))?;

        let mut body = Vec::with_capacity(16 + 4 + 1 + UNCOMPRESSED_POINT_LEN + ciphertext.len());
        body.extend_from_slice(&salt);
        body.extend_from_slice(&RECORD_SIZE.to_be_bytes());
        body.push(UNCOMPRESSED_POINT_LEN as u8);
        body.extend_from_slice(&as_public);
        body.extend_from_slice(&ciphertext);
        Ok(body)
    }

    /// `vapid t=<jwt>, k=<public key>` for one push service origin.
    pub fn vapid_authorization(&self, endpoint: &str) -> Result<String, ProviderError> {
        let authorization = self.vapid_token(endpoint)?;
        let key = if self.vapid_public_key.is_empty() {
            let secret = decode_b64url(&self.vapid_private_key)?;
            let secret = SecretKey::from_slice(&secret)
                .map_err(|e| ProviderError::Failed(format!("vapid key: {e}")))?;
            encode_b64url(&uncompressed(&secret.public_key()))
        } else {
            self.vapid_public_key.clone()
        };
        Ok(format!("vapid t={authorization}, k={key}"))
    }

    /// ES256 JWT with `aud` (the push service origin), `exp` and `sub`.
    pub fn vapid_token(&self, endpoint: &str) -> Result<String, ProviderError> {
        let audience = endpoint_origin(endpoint)?;
        let subject = if self.subscriber.starts_with("mailto:") {
            self.subscriber.clone()
        } else {
            format!("mailto:{}", self.subscriber)
        };
        let expires = now_unix() + VAPID_TTL_SECONDS;

        let header = serde_json::json!({"typ": "JWT", "alg": "ES256"});
        let claims = serde_json::json!({
            "aud": audience,
            "exp": expires,
            "sub": subject,
        });
        let signing_input = format!(
            "{}.{}",
            encode_b64url(header.to_string().as_bytes()),
            encode_b64url(claims.to_string().as_bytes())
        );

        let secret = decode_b64url(&self.vapid_private_key)?;
        let signing_key = SigningKey::from_slice(&secret)
            .map_err(|e| ProviderError::Failed(format!("vapid key: {e}")))?;
        let signature: Signature = signing_key.sign(signing_input.as_bytes());

        Ok(format!(
            "{signing_input}.{}",
            encode_b64url(&signature.to_bytes())
        ))
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

pub type SharedWebPush = Arc<WebPushNotifier>;

fn encode_b64url(data: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

fn decode_b64url(text: &str) -> Result<Vec<u8>, ProviderError> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(text.trim())
        .map_err(|e| ProviderError::Failed(format!("web push key: {e}")))
}

fn random_array<const N: usize>() -> Result<[u8; N], ProviderError> {
    let mut bytes = [0_u8; N];
    getrandom::fill(&mut bytes).map_err(|e| ProviderError::Failed(format!("random: {e}")))?;
    Ok(bytes)
}

fn random_secret() -> Result<SecretKey, ProviderError> {
    // A random 32-byte string is not always a valid scalar; retry.
    for _ in 0..8 {
        let bytes = random_array::<32>()?;
        if let Ok(secret) = SecretKey::from_slice(&bytes) {
            return Ok(secret);
        }
    }
    Err(ProviderError::Failed("cannot generate a P-256 key".into()))
}

/// Uncompressed SEC1 point (`0x04 || x || y`), which is what a browser's
/// subscription uses.
fn uncompressed(public: &PublicKey) -> Vec<u8> {
    Sec1Point::<p256::NistP256>::from(*public)
        .as_bytes()
        .to_vec()
}

fn endpoint_origin(endpoint: &str) -> Result<String, ProviderError> {
    let url = reqwest::Url::parse(endpoint)
        .map_err(|e| ProviderError::Failed(format!("push endpoint: {e}")))?;
    let host = url
        .host_str()
        .ok_or_else(|| ProviderError::Failed("push endpoint has no host".into()))?;
    Ok(match url.port() {
        Some(port) => format!("{}://{}:{}", url.scheme(), host, port),
        None => format!("{}://{}", url.scheme(), host),
    })
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

#[allow(dead_code)]
fn _app_result(_r: AppResult<()>) {}

#[cfg(test)]
mod tests;
