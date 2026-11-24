//! Pending audio triage: `classify:pending:<user>` with a short TTL,
//! single-use (`classify_store.go`).
//!
//! `POST /api/sos/classify` writes it, and the next `POST /api/sos` of the
//! same user consumes it, so the first fan-out already uses the audio-derived
//! level without trusting a client-sent severity.

use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;

use crate::error::{AppError, AppResult};
use crate::model::sos::TriageInfo;
use crate::repository::redis_conn::RedisHandle;

pub const CLASSIFY_KEY_PREFIX: &str = "classify:pending:";
pub const DEFAULT_CLASSIFY_TTL: Duration = Duration::from_secs(2 * 60);

#[async_trait]
pub trait PendingClassify: Send + Sync {
    async fn save(&self, user_id: &str, info: &TriageInfo) -> AppResult<()>;
    /// Consume: the value is deleted, so one clip classifies one alert.
    async fn take(&self, user_id: &str) -> AppResult<Option<TriageInfo>>;
}

pub struct RedisClassifyStore {
    redis: RedisHandle,
    ttl: Duration,
}

impl RedisClassifyStore {
    pub fn new(redis: RedisHandle, ttl: Duration) -> Self {
        Self {
            redis,
            ttl: if ttl.is_zero() {
                DEFAULT_CLASSIFY_TTL
            } else {
                ttl
            },
        }
    }
}

#[async_trait]
impl PendingClassify for RedisClassifyStore {
    async fn save(&self, user_id: &str, info: &TriageInfo) -> AppResult<()> {
        let payload = serde_json::to_string(info)?;
        let mut conn = self.redis.conn().await?;
        let _: () = conn
            .set_ex(
                format!("{CLASSIFY_KEY_PREFIX}{user_id}"),
                payload,
                self.ttl.as_secs().max(1),
            )
            .await
            .map_err(|e| AppError::Unavailable(format!("store classify result: {e}")))?;
        Ok(())
    }

    async fn take(&self, user_id: &str) -> AppResult<Option<TriageInfo>> {
        let key = format!("{CLASSIFY_KEY_PREFIX}{user_id}");
        let mut conn = self.redis.conn().await?;

        let raw: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| AppError::Unavailable(format!("read classify result: {e}")))?;
        let Some(raw) = raw else {
            return Ok(None);
        };

        let _: () = conn
            .del(&key)
            .await
            .map_err(|e| AppError::Unavailable(format!("consume classify result: {e}")))?;

        Ok(serde_json::from_str(&raw).ok())
    }
}
