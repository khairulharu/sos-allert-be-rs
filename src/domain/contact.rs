//! `contacts` and `addresses` tables.

use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Contact {
    pub id: String,
    pub first_name: String,
    pub last_name: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub user_id: String,
    pub created_at: i64,
    pub updated_at: i64,
}
