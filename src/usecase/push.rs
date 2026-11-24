//! Browser Web Push subscriptions + the endpoint allowlist
//! (`push_usecase.go`, `push_endpoint.go`).

use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;
use validator::Validate;

use crate::domain::PushSubscription;
use crate::error::{AppError, AppResult};
use crate::model::push::{PushSubscribeRequest, PushSubscriptionResponse, PushUnsubscribeRequest};

/// Push services browsers actually hand out. Any other endpoint host is
/// refused, so a stored subscription can never make the server POST to an
/// attacker-chosen host (stored SSRF).
pub const DEFAULT_WEB_PUSH_HOSTS: &[&str] = &[
    "fcm.googleapis.com",
    "updates.push.services.mozilla.com",
    "web.push.apple.com",
];

pub fn hosts_or(allowed: &[String]) -> Vec<String> {
    if allowed.is_empty() {
        DEFAULT_WEB_PUSH_HOSTS
            .iter()
            .map(|h| (*h).to_owned())
            .collect()
    } else {
        allowed.to_vec()
    }
}

/// `https` only, and the host must be on the allowlist. An entry that starts
/// with `.` matches a suffix (push services use per-shard subdomains).
pub fn is_allowed_endpoint(raw_url: &str, allowed: &[String]) -> bool {
    let Ok(parsed) = reqwest::Url::parse(raw_url) else {
        return false;
    };
    if parsed.scheme() != "https" {
        return false;
    }
    let Some(host) = parsed.host_str() else {
        return false;
    };
    let host = host.to_lowercase();

    allowed.iter().any(|entry| {
        let entry = entry.trim().to_lowercase();
        if entry.is_empty() {
            return false;
        }
        if let Some(suffix) = entry.strip_prefix('.') {
            return host.ends_with(&format!(".{suffix}"));
        }
        host == entry
    })
}

#[async_trait]
pub trait PushSubscriptionStore: Send + Sync {
    /// Store or refresh a subscription and return the row id that actually
    /// exists (a re-subscribe keeps the original id). An endpoint belongs to
    /// the browser that registered it first: another user gets `Conflict`.
    async fn upsert(&self, subscription: &PushSubscription) -> AppResult<String>;
    async fn list_by_user(&self, user_id: &str) -> AppResult<Vec<PushSubscription>>;
    async fn delete_by_user_and_endpoint(&self, user_id: &str, endpoint: &str) -> AppResult<()>;
    async fn delete_by_endpoint(&self, endpoint: &str) -> AppResult<()>;
}

pub struct PushUseCase {
    subs: Arc<dyn PushSubscriptionStore>,
    public_key: String,
    enabled: bool,
    allowed_hosts: Vec<String>,
    now_millis: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl PushUseCase {
    pub fn new(
        subs: Arc<dyn PushSubscriptionStore>,
        public_key: String,
        enabled: bool,
        allowed_hosts: Vec<String>,
    ) -> Self {
        Self {
            subs,
            public_key,
            enabled,
            allowed_hosts: hosts_or(&allowed_hosts),
            now_millis: Arc::new(crate::usecase::auth::default_now_millis),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.now_millis = clock;
        self
    }

    /// The application server key the browser needs, `503` when the channel
    /// is off.
    pub fn vapid_public_key(&self) -> AppResult<String> {
        if !self.enabled || self.public_key.is_empty() {
            return Err(AppError::Unavailable("web push is not configured".into()));
        }
        Ok(self.public_key.clone())
    }

    pub async fn subscribe(
        &self,
        user_id: &str,
        request: PushSubscribeRequest,
    ) -> AppResult<PushSubscriptionResponse> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;

        if !is_allowed_endpoint(&request.endpoint, &self.allowed_hosts) {
            return Err(AppError::BadRequest(
                "push endpoint must be an https URL on an allowed push service".into(),
            ));
        }

        let now = (self.now_millis)();
        let subscription = PushSubscription {
            id: Uuid::new_v4().to_string(),
            user_id: user_id.to_owned(),
            endpoint: request.endpoint,
            p256dh: request.p256dh,
            auth: request.auth,
            user_agent: (!request.user_agent.is_empty()).then_some(request.user_agent),
            created_at: now,
            updated_at: now,
        };

        let id = self.subs.upsert(&subscription).await?;

        Ok(PushSubscriptionResponse {
            id,
            endpoint: subscription.endpoint,
            created_at: now,
            updated_at: now,
        })
    }

    /// Unknown endpoints are a no-op, so the call is idempotent.
    pub async fn unsubscribe(
        &self,
        user_id: &str,
        request: PushUnsubscribeRequest,
    ) -> AppResult<bool> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        self.subs
            .delete_by_user_and_endpoint(user_id, &request.endpoint)
            .await?;
        Ok(true)
    }

    pub async fn list(&self, user_id: &str) -> AppResult<Vec<PushSubscriptionResponse>> {
        let rows = self.subs.list_by_user(user_id).await?;
        Ok(rows
            .iter()
            .map(|sub| PushSubscriptionResponse {
                id: sub.id.clone(),
                endpoint: sub.endpoint.clone(),
                created_at: sub.created_at,
                updated_at: sub.updated_at,
            })
            .collect())
    }
}
