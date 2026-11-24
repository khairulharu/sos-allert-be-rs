//! `CachedSessionStore` behaviour without Redis: hit, expiry, single-session
//! replacement, eviction and the "never cache an error" rule.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::error::{AppError, AppResult};
use crate::repository::session::CachedSessionStore;
use crate::usecase::auth::SessionStore;

#[derive(Default)]
struct FakeSessions {
    tokens: Mutex<HashMap<String, String>>,
    gets: AtomicU64,
    creates: AtomicU64,
    deletes: AtomicU64,
    fail_get: Mutex<bool>,
}

impl FakeSessions {
    fn count_gets(&self) -> u64 {
        self.gets.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SessionStore for FakeSessions {
    async fn create(&self, user_id: &str, token: &str, _ttl: Duration) -> AppResult<()> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        // Single session: drop the user's previous token.
        let mut tokens = self.tokens.lock().unwrap();
        tokens.retain(|_, value| value != user_id);
        tokens.insert(token.to_owned(), user_id.to_owned());
        Ok(())
    }

    async fn get(&self, token: &str) -> AppResult<Option<String>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        if *self.fail_get.lock().unwrap() {
            return Err(AppError::Unavailable("down".into()));
        }
        Ok(self.tokens.lock().unwrap().get(token).cloned())
    }

    async fn delete_by_user(&self, user_id: &str) -> AppResult<()> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        self.tokens
            .lock()
            .unwrap()
            .retain(|_, value| value != user_id);
        Ok(())
    }
}

fn cache_with_clock(ttl: Duration) -> (Arc<CachedSessionStore>, Arc<FakeSessions>, Arc<AtomicU64>) {
    let fake = Arc::new(FakeSessions::default());
    let now = Arc::new(AtomicU64::new(1_000));
    let clock_now = now.clone();
    let cache = Arc::new(
        CachedSessionStore::new(fake.clone() as Arc<dyn SessionStore>, ttl)
            .with_clock(Arc::new(move || clock_now.load(Ordering::SeqCst))),
    );
    (cache, fake, now)
}

#[tokio::test]
async fn second_lookup_is_served_from_cache() {
    let (cache, fake, _) = cache_with_clock(Duration::from_secs(30));
    cache
        .create("user-1", "tok-1", Duration::from_secs(60))
        .await
        .unwrap();

    assert_eq!(cache.get("tok-1").await.unwrap().as_deref(), Some("user-1"));
    assert_eq!(cache.get("tok-1").await.unwrap().as_deref(), Some("user-1"));
    assert_eq!(fake.count_gets(), 0, "create primed the cache");
}

#[tokio::test]
async fn unknown_token_is_not_cached() {
    let (cache, fake, _) = cache_with_clock(Duration::from_secs(30));
    assert!(cache.get("nope").await.unwrap().is_none());
    assert!(cache.get("nope").await.unwrap().is_none());
    assert_eq!(fake.count_gets(), 2, "a miss must always reach Redis");
}

#[tokio::test]
async fn entry_expires_after_ttl() {
    let (cache, fake, now) = cache_with_clock(Duration::from_secs(10));
    cache
        .create("user-1", "tok-1", Duration::from_secs(60))
        .await
        .unwrap();
    assert!(cache.get("tok-1").await.unwrap().is_some());
    assert_eq!(fake.count_gets(), 0);

    now.fetch_add(10_000, Ordering::SeqCst);
    assert_eq!(cache.get("tok-1").await.unwrap().as_deref(), Some("user-1"));
    assert_eq!(fake.count_gets(), 1, "expired entry falls through to Redis");
}

#[tokio::test]
async fn login_replaces_previous_token_locally() {
    let (cache, fake, _) = cache_with_clock(Duration::from_secs(30));
    cache
        .create("user-1", "tok-old", Duration::from_secs(60))
        .await
        .unwrap();
    cache
        .create("user-1", "tok-new", Duration::from_secs(60))
        .await
        .unwrap();

    assert!(cache.get("tok-old").await.unwrap().is_none());
    assert_eq!(
        cache.get("tok-new").await.unwrap().as_deref(),
        Some("user-1")
    );
    assert_eq!(fake.count_gets(), 1, "only the stale token went to Redis");
}

#[tokio::test]
async fn evict_drops_token_unless_it_is_kept() {
    let (cache, fake, _) = cache_with_clock(Duration::from_secs(30));
    cache
        .create("user-1", "tok-1", Duration::from_secs(60))
        .await
        .unwrap();

    cache.evict("user-1", Some("tok-1"));
    assert_eq!(cache.get("tok-1").await.unwrap().as_deref(), Some("user-1"));

    // Eviction only drops the local entry: Redis is still the source of truth,
    // so the next lookup reaches it and repopulates the cache.
    cache.evict("user-1", None);
    assert_eq!(cache.get("tok-1").await.unwrap().as_deref(), Some("user-1"));
    assert_eq!(
        fake.count_gets(),
        1,
        "evicted token must be re-read from Redis"
    );

    // Once Redis no longer has it either, verification fails.
    fake.tokens.lock().unwrap().clear();
    cache.evict("user-1", None);
    assert!(cache.get("tok-1").await.unwrap().is_none());
}

#[tokio::test]
async fn redis_error_is_never_cached() {
    let (cache, fake, _) = cache_with_clock(Duration::from_secs(30));
    *fake.fail_get.lock().unwrap() = true;

    assert!(cache.get("tok-1").await.is_err());
    *fake.fail_get.lock().unwrap() = false;
    fake.tokens
        .lock()
        .unwrap()
        .insert("tok-1".into(), "user-1".into());

    // The first error must not have been remembered as "unknown token".
    assert_eq!(cache.get("tok-1").await.unwrap().as_deref(), Some("user-1"));
}

#[tokio::test]
async fn zero_ttl_disables_the_cache() {
    let (cache, fake, _) = cache_with_clock(Duration::ZERO);
    cache
        .create("user-1", "tok-1", Duration::from_secs(60))
        .await
        .unwrap();
    assert!(cache.get("tok-1").await.unwrap().is_some());
    assert_eq!(fake.count_gets(), 1);
}

#[tokio::test]
async fn logout_removes_both_redis_and_cache_entries() {
    let (cache, fake, _) = cache_with_clock(Duration::from_secs(30));
    cache
        .create("user-1", "tok-1", Duration::from_secs(60))
        .await
        .unwrap();
    cache.delete_by_user("user-1").await.unwrap();

    assert_eq!(fake.deletes.load(Ordering::SeqCst), 1);
    assert!(cache.get("tok-1").await.unwrap().is_none());
}
