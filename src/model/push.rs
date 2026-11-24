//! Web Push subscription models (`push_model.go`).

use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct PushSubscribeRequest {
    /// The browser's `PushSubscription.endpoint`; it becomes an outbound POST
    /// target, so it must be an allowlisted https push service.
    #[validate(length(min = 1, max = 512))]
    pub endpoint: String,
    #[validate(length(min = 1, max = 255))]
    pub p256dh: String,
    #[validate(length(min = 1, max = 255))]
    pub auth: String,
    #[serde(default)]
    #[validate(length(max = 255))]
    pub user_agent: String,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct PushUnsubscribeRequest {
    #[validate(length(min = 1, max = 512))]
    pub endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushSubscriptionResponse {
    pub id: String,
    pub endpoint: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushVapidKeyResponse {
    pub public_key: String,
}
