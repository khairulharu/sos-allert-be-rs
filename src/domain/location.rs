//! `locations` table.
//!
//! Historical row per user (`uq_locations_user_id`), upserted on every
//! heartbeat. Live positions for radius search live in Redis (`users:locations`
//! GEO set), not here — same split as the Go service.

use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// A Redis-resolved or request-supplied position.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocationCache {
    pub user_id: String,
    /// Redis `GEOADD` takes longitude first; keep that order everywhere.
    pub longitude: f64,
    pub latitude: f64,
}

impl LocationCache {
    pub fn new(user_id: impl Into<String>, longitude: f64, latitude: f64) -> Self {
        Self {
            user_id: user_id.into(),
            longitude,
            latitude,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Location {
    pub id: String,
    pub user_id: String,
    pub longitude: f64,
    pub latitude: f64,
    pub created_at: i64,
    pub updated_at: i64,
}
