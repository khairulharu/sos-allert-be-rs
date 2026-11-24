//! Redis idempotency store for SOS triggers: `idempotency:sos:<key>` -> alert id.
//!
//! `Release` is a compare-and-delete on the pending marker only, so a failed
//! trigger can hand its claim back without discarding a finished result.

use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;

use crate::error::{AppError, AppResult};
use crate::repository::redis_conn::RedisHandle;
use crate::usecase::sos::SosIdempotency;

pub const IDEMPOTENCY_PREFIX: &str = "idempotency:sos:";
pub const IDEMPOTENCY_TTL: Duration = Duration::from_secs(5 * 60);
const PENDING: &str = "pending";

const RELEASE_SCRIPT: &str = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
"#;

#[derive(Clone)]
pub struct RedisIdempotencyRepository {
    redis: RedisHandle,
}

impl RedisIdempotencyRepository {
    pub fn new(redis: RedisHandle) -> Self {
        Self { redis }
    }
}

#[async_trait]
impl SosIdempotency for RedisIdempotencyRepository {
    async fn claim(&self, key: &str) -> AppResult<bool> {
        let mut conn = self.redis.conn().await?;
        let ok: bool = redis::cmd("SET")
            .arg(format!("{IDEMPOTENCY_PREFIX}{key}"))
            .arg(PENDING)
            .arg("NX")
            .arg("PX")
            .arg(IDEMPOTENCY_TTL.as_millis() as i64)
            .query_async(&mut conn)
            .await
            .map(|value: Option<String>| value.is_some())
            .map_err(|e| AppError::Unavailable(format!("redis idempotency claim: {e}")))?;
        Ok(ok)
    }

    async fn get(&self, key: &str) -> AppResult<Option<String>> {
        let mut conn = self.redis.conn().await?;
        let value: Option<String> = conn
            .get(format!("{IDEMPOTENCY_PREFIX}{key}"))
            .await
            .map_err(|e| AppError::Unavailable(format!("redis idempotency get: {e}")))?;
        Ok(value.filter(|value| value != PENDING))
    }

    async fn set(&self, key: &str, value: &str) -> AppResult<()> {
        let mut conn = self.redis.conn().await?;
        let _: () = conn
            .set_ex(
                format!("{IDEMPOTENCY_PREFIX}{key}"),
                value,
                IDEMPOTENCY_TTL.as_secs(),
            )
            .await
            .map_err(|e| AppError::Unavailable(format!("redis idempotency set: {e}")))?;
        Ok(())
    }

    async fn release(&self, key: &str) -> AppResult<()> {
        let mut conn = self.redis.conn().await?;
        let script = redis::Script::new(RELEASE_SCRIPT);
        let _: i64 = script
            .key(format!("{IDEMPOTENCY_PREFIX}{key}"))
            .arg(PENDING)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| AppError::Unavailable(format!("redis idempotency release: {e}")))?;
        Ok(())
    }
}
