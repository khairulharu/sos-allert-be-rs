//! `addresses` table.

use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Address {
    pub id: String,
    pub contact_id: String,
    pub street: Option<String>,
    pub city: Option<String>,
    pub province: Option<String>,
    pub postal_code: Option<String>,
    pub country: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}
