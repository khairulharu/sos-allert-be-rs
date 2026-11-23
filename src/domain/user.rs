//! `users` table.

use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct User {
    pub id: String,
    pub name: String,
    /// bcrypt hash; never returned to clients.
    pub password: String,
    pub created_at: i64,
    pub updated_at: i64,
    /// bcrypt hash of the safe PIN, `None` until the user sets one.
    pub safe_pin_hash: Option<String>,
    pub fcm_token: Option<String>,
    pub apns_token: Option<String>,
}
