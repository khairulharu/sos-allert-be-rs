//! `push_subscriptions` table (browser Web Push / VAPID).

use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct PushSubscription {
    pub id: String,
    pub user_id: String,
    /// Unique: the same browser re-subscribing re-points this row.
    pub endpoint: String,
    pub p256dh: String,
    pub auth: String,
    pub user_agent: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}
