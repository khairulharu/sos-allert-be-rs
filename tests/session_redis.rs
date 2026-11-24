//! Redis session store + cache invalidation against a live Redis.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use sos_allert::repository::redis_conn::RedisHandle;
use sos_allert::repository::session::{run_invalidation, CachedSessionStore, RedisSessionStore};
use sos_allert::usecase::auth::SessionStore;

fn store() -> Option<RedisSessionStore> {
    let config = test_config()?;
    let client = redis::Client::open(config.redis.url()).ok()?;
    Some(RedisSessionStore::new(RedisHandle::new(client)))
}

#[tokio::test]
async fn session_round_trip_and_single_session_replace() {
    let Some(store) = store() else { return };
    let user = unique_id("rs-sess");

    store
        .create(&user, "tok-a", Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(
        store.get("tok-a").await.unwrap().as_deref(),
        Some(user.as_str())
    );

    store
        .create(&user, "tok-b", Duration::from_secs(60))
        .await
        .unwrap();
    assert!(
        store.get("tok-a").await.unwrap().is_none(),
        "previous token was not dropped"
    );
    assert_eq!(
        store.get("tok-b").await.unwrap().as_deref(),
        Some(user.as_str())
    );

    store.delete_by_user(&user).await.unwrap();
    assert!(store.get("tok-b").await.unwrap().is_none());
}

#[tokio::test]
async fn session_expires_with_its_ttl() {
    let Some(store) = store() else { return };
    let user = unique_id("rs-ttl");

    store
        .create(&user, "tok-ttl", Duration::from_secs(1))
        .await
        .unwrap();
    assert!(store.get("tok-ttl").await.unwrap().is_some());

    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(
        store.get("tok-ttl").await.unwrap().is_none(),
        "session outlived its TTL"
    );
}

#[tokio::test]
async fn invalidation_reaches_another_instance_cache() {
    let Some(store) = store() else { return };
    let store = Arc::new(store);
    let user = unique_id("rs-inval");

    let cache_a = Arc::new(
        CachedSessionStore::new(
            store.clone() as Arc<dyn SessionStore>,
            Duration::from_secs(30),
        )
        .with_publisher(store.clone()),
    );
    let cache_b = Arc::new(
        CachedSessionStore::new(
            store.clone() as Arc<dyn SessionStore>,
            Duration::from_secs(30),
        )
        .with_publisher(store.clone()),
    );

    // Instance B must be listening before A publishes.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let redis_handle = store.handle().clone();
    let cache_for_task = cache_b.clone();
    let task = tokio::spawn(async move {
        run_invalidation(redis_handle, cache_for_task, shutdown_rx).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // A logs in, B caches the token.
    cache_a
        .create(&user, "tok-1", Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(
        cache_b.get("tok-1").await.unwrap().as_deref(),
        Some(user.as_str()),
        "B did not cache the session"
    );

    // A logs in again: the old token is dropped in Redis and announced.
    cache_a
        .create(&user, "tok-2", Duration::from_secs(60))
        .await
        .unwrap();

    let mut dropped = false;
    for _ in 0..40 {
        if cache_b.get("tok-1").await.unwrap().is_none() {
            dropped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(dropped, "instance B kept serving the invalidated token");
    assert_eq!(
        cache_b.get("tok-2").await.unwrap().as_deref(),
        Some(user.as_str()),
        "B cannot see the new token"
    );

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
}

#[tokio::test]
async fn logout_evicts_the_other_instance() {
    let Some(store) = store() else { return };
    let store = Arc::new(store);
    let user = unique_id("rs-logout");

    let cache_a = Arc::new(
        CachedSessionStore::new(
            store.clone() as Arc<dyn SessionStore>,
            Duration::from_secs(30),
        )
        .with_publisher(store.clone()),
    );
    let cache_b = Arc::new(
        CachedSessionStore::new(
            store.clone() as Arc<dyn SessionStore>,
            Duration::from_secs(30),
        )
        .with_publisher(store.clone()),
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let redis_handle = store.handle().clone();
    let cache_for_task = cache_b.clone();
    let task = tokio::spawn(async move {
        run_invalidation(redis_handle, cache_for_task, shutdown_rx).await;
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    cache_a
        .create(&user, "tok-x", Duration::from_secs(60))
        .await
        .unwrap();
    assert!(cache_b.get("tok-x").await.unwrap().is_some());

    cache_a.delete_by_user(&user).await.unwrap();

    let mut gone = false;
    for _ in 0..40 {
        if cache_b.get("tok-x").await.unwrap().is_none() {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(gone, "logout did not reach the second instance");

    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
}
