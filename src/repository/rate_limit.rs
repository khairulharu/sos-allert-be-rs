//! Redis sliding-window rate limiters (ported scripts).
//!
//! * `auth` - per client IP, `auth.rate_limit_per_minute` per minute, then a
//!   `auth.lockout_minutes` lockout. The middleware fails **open** on errors so
//!   a Redis outage cannot lock everyone out of login.
//! * `sos` / `classify` - per user; added in phase 3, the limiter shares the
//!   same script.

use std::time::Duration;

use crate::error::{AppError, AppResult};
use crate::repository::redis_conn::RedisHandle;

pub const AUTH_RATE_LIMIT_PREFIX: &str = "ratelimit:auth:";
pub const AUTH_LOCKOUT_PREFIX: &str = "lockout:auth:";

const AUTH_RATE_LIMIT_SCRIPT: &str = r#"
local windowKey = KEYS[1]
local lockKey = KEYS[2]
local now = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local lockoutMs = tonumber(ARGV[4])
local member = ARGV[5]

local locked = redis.call("PTTL", lockKey)
if locked > 0 then
  return {0, locked}
end

redis.call("ZREMRANGEBYSCORE", windowKey, 0, now - windowMs)
local count = redis.call("ZCARD", windowKey)
if count >= limit then
  redis.call("SET", lockKey, "1", "PX", lockoutMs)
  return {0, lockoutMs}
end

redis.call("ZADD", windowKey, now, member)
redis.call("PEXPIRE", windowKey, windowMs)
return {1, 0}
"#;

#[derive(Clone)]
pub struct RedisRateLimitRepository {
    redis: RedisHandle,
}

impl RedisRateLimitRepository {
    pub fn new(redis: RedisHandle) -> Self {
        Self { redis }
    }

    /// `(allowed, retry_after)`. `retry_after` is zero when allowed.
    pub async fn allow_auth(
        &self,
        ip: &str,
        limit: u32,
        window: Duration,
        lockout: Duration,
    ) -> AppResult<(bool, Duration)> {
        let now = now_millis();
        let member = format!("{now}-{}", now_nanos());
        let mut conn = self.redis.conn().await?;

        let script = redis::Script::new(AUTH_RATE_LIMIT_SCRIPT);
        let reply: Vec<i64> = script
            .key(format!("{AUTH_RATE_LIMIT_PREFIX}{ip}"))
            .key(format!("{AUTH_LOCKOUT_PREFIX}{ip}"))
            .arg(now)
            .arg(window.as_millis() as i64)
            .arg(limit as i64)
            .arg(lockout.as_millis() as i64)
            .arg(member)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| AppError::Unavailable(format!("redis auth rate limit: {e}")))?;

        match reply.as_slice() {
            [allowed, retry_ms] => Ok((*allowed == 1, Duration::from_millis(*retry_ms as u64))),
            other => Err(AppError::Unavailable(format!(
                "redis auth rate limit: unexpected reply {other:?}"
            ))),
        }
    }
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn now_nanos() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}
