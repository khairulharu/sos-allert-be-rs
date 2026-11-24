//! Redis-backed session store plus the in-process verification cache.
//!
//! Keys, Lua scripts and invalidation behaviour are ported from the Go
//! `session_repository.go` / `session_cache.go`:
//!
//! * `auth:token:<token>` -> user id
//! * `auth:user:<user_id>` -> token (single session: the latest login wins)
//! * `auth:session_invalidate` pub/sub carries login/logout changes so a
//!   cached token is dropped on every instance immediately.
//!
//! The cache never caches errors: a Redis outage must keep surfacing as `503`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use redis::AsyncCommands;

use crate::error::AppResult;
use crate::repository::redis_conn::RedisHandle;
use crate::usecase::auth::SessionStore;

pub const SESSION_INVALIDATION_CHANNEL: &str = "auth:session_invalidate";
pub const AUTH_TOKEN_PREFIX: &str = "auth:token:";
pub const AUTH_USER_PREFIX: &str = "auth:user:";

/// Default cap for the in-process cache (`defaultSessionCacheEntries`).
pub const DEFAULT_SESSION_CACHE_ENTRIES: usize = 10_000;

const SESSION_CREATE_SCRIPT: &str = r#"
local old = redis.call("GET", KEYS[2])
if old and ARGV[4] and old ~= ARGV[2] then
  redis.call("DEL", ARGV[4] .. old)
end
if ARGV[3] == "0" then
  redis.call("SET", KEYS[1], ARGV[1])
  redis.call("SET", KEYS[2], ARGV[2])
else
  redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[3])
  redis.call("SET", KEYS[2], ARGV[2], "EX", ARGV[3])
end
return 1
"#;

const SESSION_DELETE_SCRIPT: &str = r#"
local token = redis.call("GET", KEYS[1])
if token and ARGV[1] then
  redis.call("DEL", ARGV[1] .. token)
end
redis.call("DEL", KEYS[1])
return 1
"#;

#[derive(Clone)]
pub struct RedisSessionStore {
    redis: RedisHandle,
}

impl RedisSessionStore {
    pub fn new(redis: RedisHandle) -> Self {
        Self { redis }
    }

    pub fn handle(&self) -> &RedisHandle {
        &self.redis
    }

    /// Publish a session change; failure only means other instances keep a
    /// stale entry until its TTL, so the caller's login/logout still succeeds.
    pub async fn publish_invalidation(
        &self,
        user_id: &str,
        keep_token: Option<&str>,
    ) -> AppResult<()> {
        let payload = serde_json::json!({
            "user_id": user_id,
            "keep_token": keep_token.unwrap_or(""),
        });

        let mut conn = self.redis.conn().await?;
        let _: i64 = redis::cmd("PUBLISH")
            .arg(SESSION_INVALIDATION_CHANNEL)
            .arg(payload.to_string())
            .query_async(&mut conn)
            .await
            .map_err(|e| crate::error::AppError::Unavailable(format!("redis publish: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl SessionStore for RedisSessionStore {
    async fn create(&self, user_id: &str, token: &str, ttl: Duration) -> AppResult<()> {
        let seconds = if ttl.is_zero() { 0 } else { ttl.as_secs() };
        let mut conn = self.redis.conn().await?;

        let script = redis::Script::new(SESSION_CREATE_SCRIPT);
        let _: i64 = script
            .key(format!("{AUTH_TOKEN_PREFIX}{token}"))
            .key(format!("{AUTH_USER_PREFIX}{user_id}"))
            .arg(user_id)
            .arg(token)
            .arg(seconds)
            .arg(AUTH_TOKEN_PREFIX)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| {
                crate::error::AppError::Unavailable(format!("redis session create: {e}"))
            })?;
        Ok(())
    }

    async fn get(&self, token: &str) -> AppResult<Option<String>> {
        let mut conn = self.redis.conn().await?;
        let value: Option<String> = conn
            .get(format!("{AUTH_TOKEN_PREFIX}{token}"))
            .await
            .map_err(|e| crate::error::AppError::Unavailable(format!("redis session get: {e}")))?;
        Ok(value)
    }

    async fn delete_by_user(&self, user_id: &str) -> AppResult<()> {
        let mut conn = self.redis.conn().await?;
        let script = redis::Script::new(SESSION_DELETE_SCRIPT);
        let _: i64 = script
            .key(format!("{AUTH_USER_PREFIX}{user_id}"))
            .arg(AUTH_TOKEN_PREFIX)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| {
                crate::error::AppError::Unavailable(format!("redis session delete: {e}"))
            })?;
        Ok(())
    }
}

struct CacheEntry {
    user_id: String,
    expires_at_ms: u64,
}

/// Milestone clock, in milliseconds: replaceable so tests do not sleep.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

pub struct CachedSessionStore {
    inner: Arc<dyn SessionStore>,
    publisher: Option<Arc<RedisSessionStore>>,
    ttl_ms: u64,
    max_entries: usize,
    clock: Clock,
    by_token: Mutex<HashMap<String, CacheEntry>>,
    by_user: Mutex<HashMap<String, String>>,
}

impl CachedSessionStore {
    /// `ttl` <= 0 disables the cache entirely (always reads Redis).
    pub fn new(inner: Arc<dyn SessionStore>, ttl: Duration) -> Self {
        Self {
            inner,
            publisher: None,
            ttl_ms: ttl.as_millis() as u64,
            max_entries: DEFAULT_SESSION_CACHE_ENTRIES,
            clock: Arc::new(default_clock_ms),
            by_token: Mutex::new(HashMap::new()),
            by_user: Mutex::new(HashMap::new()),
        }
    }

    /// Attach the Redis store so login/logout can announce invalidation.
    pub fn with_publisher(mut self, publisher: Arc<RedisSessionStore>) -> Self {
        self.publisher = Some(publisher);
        self
    }

    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = max;
        self
    }

    pub fn enabled(&self) -> bool {
        self.ttl_ms > 0
    }

    /// Drop a user's cached token unless it is `keep_token`.
    pub fn evict(&self, user_id: &str, keep_token: Option<&str>) {
        let mut by_user = self.by_user.lock().expect("session cache poisoned");
        let token = match by_user.get(user_id) {
            Some(token) => token.clone(),
            None => return,
        };
        if Some(token.as_str()) == keep_token {
            return;
        }
        by_user.remove(user_id);
        self.by_token
            .lock()
            .expect("session cache poisoned")
            .remove(&token);
    }

    fn lookup(&self, token: &str) -> Option<String> {
        let now = (self.clock)();
        let mut by_token = self.by_token.lock().expect("session cache poisoned");
        let entry = by_token.get(token)?;
        if now >= entry.expires_at_ms {
            let user_id = entry.user_id.clone();
            by_token.remove(token);
            drop(by_token);
            let mut by_user = self.by_user.lock().expect("session cache poisoned");
            if by_user.get(&user_id).map(String::as_str) == Some(token) {
                by_user.remove(&user_id);
            }
            return None;
        }
        Some(entry.user_id.clone())
    }

    fn store(&self, token: &str, user_id: &str) {
        let mut by_user = self.by_user.lock().expect("session cache poisoned");
        // A newer session may have been recorded mid-flight; do not resurrect
        // the old token.
        if let Some(current) = by_user.get(user_id) {
            if current != token {
                return;
            }
        }

        let mut by_token = self.by_token.lock().expect("session cache poisoned");
        if by_token.len() >= self.max_entries {
            drop(by_token);
            drop(by_user);
            self.make_room();
            by_user = self.by_user.lock().expect("session cache poisoned");
            by_token = self.by_token.lock().expect("session cache poisoned");
        }

        let expires_at_ms = (self.clock)() + self.ttl_ms;
        by_token.insert(
            token.to_owned(),
            CacheEntry {
                user_id: user_id.to_owned(),
                expires_at_ms,
            },
        );
        by_user.insert(user_id.to_owned(), token.to_owned());
    }

    fn replace(&self, token: &str, user_id: &str) {
        self.evict(user_id, None);
        self.store(token, user_id);
    }

    /// Free at least one slot: expired entries first, then arbitrary ones.
    fn make_room(&self) {
        let now = (self.clock)();
        let mut by_token = self.by_token.lock().expect("session cache poisoned");
        let expired: Vec<String> = by_token
            .iter()
            .filter(|(_, entry)| now >= entry.expires_at_ms)
            .map(|(token, _)| token.clone())
            .collect();
        for token in expired {
            if by_token.len() < self.max_entries {
                break;
            }
            by_token.remove(&token);
        }
        if by_token.len() >= self.max_entries {
            if let Some(token) = by_token.keys().next().cloned() {
                by_token.remove(&token);
            }
        }
    }
}

#[async_trait]
impl SessionStore for CachedSessionStore {
    async fn create(&self, user_id: &str, token: &str, ttl: Duration) -> AppResult<()> {
        self.inner.create(user_id, token, ttl).await?;
        if !self.enabled() {
            return Ok(());
        }
        self.replace(token, user_id);
        if let Some(publisher) = &self.publisher {
            if let Err(e) = publisher.publish_invalidation(user_id, Some(token)).await {
                tracing::warn!(error = %e, "session invalidation publish failed; relying on TTL");
            }
        }
        Ok(())
    }

    async fn get(&self, token: &str) -> AppResult<Option<String>> {
        if !self.enabled() {
            return self.inner.get(token).await;
        }
        if let Some(user_id) = self.lookup(token) {
            return Ok(Some(user_id));
        }
        let user_id = self.inner.get(token).await?;
        if let Some(user_id) = &user_id {
            self.store(token, user_id);
        }
        Ok(user_id)
    }

    async fn delete_by_user(&self, user_id: &str) -> AppResult<()> {
        self.inner.delete_by_user(user_id).await?;
        if !self.enabled() {
            return Ok(());
        }
        self.evict(user_id, None);
        if let Some(publisher) = &self.publisher {
            if let Err(e) = publisher.publish_invalidation(user_id, None).await {
                tracing::warn!(error = %e, "session invalidation publish failed; relying on TTL");
            }
        }
        Ok(())
    }
}

/// Subscribe to `auth:session_invalidate` and apply other instances' changes
/// until `ctx` is cancelled.
pub async fn run_invalidation(
    redis: RedisHandle,
    cache: Arc<CachedSessionStore>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    if !cache.enabled() {
        return;
    }

    let client = redis.client().clone();
    let mut pubsub = match client.get_async_pubsub().await {
        Ok(pubsub) => pubsub,
        Err(e) => {
            tracing::warn!(error = %e, "session invalidation subscribe failed; TTL-only mode");
            return;
        }
    };
    if let Err(e) = pubsub.subscribe(SESSION_INVALIDATION_CHANNEL).await {
        tracing::warn!(error = %e, "session invalidation subscribe failed; TTL-only mode");
        return;
    }

    let mut stream = pubsub.on_message();
    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            message = stream.next() => {
                let Some(message) = message else { return };
                let payload: String = match message.get_payload() {
                    Ok(payload) => payload,
                    Err(e) => {
                        tracing::warn!(error = %e, "bad invalidation payload");
                        continue;
                    }
                };
                let Ok(value) = serde_json::from_str::<serde_json::Value>(&payload) else {
                    continue;
                };
                let user_id = value.get("user_id").and_then(|v| v.as_str()).unwrap_or("");
                if user_id.is_empty() {
                    continue;
                }
                let keep = value.get("keep_token").and_then(|v| v.as_str());
                let keep = keep.filter(|k| !k.is_empty());
                cache.evict(user_id, keep);
            }
        }
    }
}

pub fn default_clock_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
