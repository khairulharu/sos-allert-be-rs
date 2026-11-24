//! Postgres location history: one row per user, upserted on heartbeat.

use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::AppResult;
use crate::usecase::location::LocationHistoryStore;

#[derive(Clone)]
pub struct PgLocationRepository {
    pool: PgPool,
}

impl PgLocationRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl LocationHistoryStore for PgLocationRepository {
    /// `uq_locations_user_id` makes this safe under concurrent heartbeats.
    async fn upsert(
        &self,
        user_id: &str,
        longitude: f64,
        latitude: f64,
        now_ms: i64,
    ) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO locations (id, user_id, longitude, latitude, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $5) \
             ON CONFLICT (user_id) DO UPDATE \
             SET longitude = EXCLUDED.longitude, \
                 latitude = EXCLUDED.latitude, \
                 updated_at = EXCLUDED.updated_at",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(user_id)
        .bind(longitude)
        .bind(latitude)
        .bind(now_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
