//! Location use case: heartbeat, staleness, nearby masking and the batched
//! last_seen read.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::domain::LocationCache;
use crate::error::{AppError, AppResult};
use crate::model::UpdateLocationRequest;
use crate::usecase::location::{
    GeoHit, LocationGeoStore, LocationHistoryStore, LocationUseCase, STALE_THRESHOLD_MS,
};

const NOW: i64 = 1_700_000_000_000;

#[derive(Default)]
struct FakeGeo {
    positions: Mutex<HashMap<String, LocationCache>>,
    last_seen: Mutex<HashMap<String, i64>>,
    hits: Mutex<Vec<GeoHit>>,
    batch_calls: AtomicU64,
}

#[async_trait]
impl LocationGeoStore for FakeGeo {
    async fn save(&self, location: &LocationCache, now_ms: i64) -> AppResult<()> {
        self.positions
            .lock()
            .unwrap()
            .insert(location.user_id.clone(), location.clone());
        self.last_seen
            .lock()
            .unwrap()
            .insert(location.user_id.clone(), now_ms);
        Ok(())
    }

    async fn get(&self, user_id: &str) -> AppResult<Option<LocationCache>> {
        Ok(self.positions.lock().unwrap().get(user_id).cloned())
    }

    async fn last_seen(&self, user_id: &str) -> AppResult<Option<i64>> {
        Ok(self.last_seen.lock().unwrap().get(user_id).copied())
    }

    async fn last_seen_batch(&self, user_ids: &[String]) -> AppResult<HashMap<String, i64>> {
        self.batch_calls.fetch_add(1, Ordering::SeqCst);
        let seen = self.last_seen.lock().unwrap();
        Ok(user_ids
            .iter()
            .filter_map(|id| seen.get(id).map(|value| (id.clone(), *value)))
            .collect())
    }

    async fn search_with_distance(
        &self,
        _longitude: f64,
        _latitude: f64,
        _radius_meters: f64,
        _count: usize,
    ) -> AppResult<Vec<GeoHit>> {
        Ok(self.hits.lock().unwrap().clone())
    }
}

#[derive(Default)]
struct FakeHistory {
    upserts: AtomicU64,
    fail: Mutex<bool>,
}

#[async_trait]
impl LocationHistoryStore for FakeHistory {
    async fn upsert(
        &self,
        _user_id: &str,
        _longitude: f64,
        _latitude: f64,
        _now_ms: i64,
    ) -> AppResult<()> {
        if *self.fail.lock().unwrap() {
            return Err(AppError::Sqlx(sqlx::Error::RowNotFound));
        }
        self.upserts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn case() -> (LocationUseCase, Arc<FakeGeo>, Arc<FakeHistory>) {
    let geo = Arc::new(FakeGeo::default());
    let history = Arc::new(FakeHistory::default());
    let case = LocationUseCase::new(
        geo.clone() as Arc<dyn LocationGeoStore>,
        Some(history.clone() as Arc<dyn LocationHistoryStore>),
    )
    .with_clock(Arc::new(|| NOW));
    (case, geo, history)
}

fn heartbeat(longitude: f64, latitude: f64) -> UpdateLocationRequest {
    UpdateLocationRequest {
        longitude: Some(longitude),
        latitude: Some(latitude),
    }
}

#[tokio::test]
async fn heartbeat_writes_redis_and_history() {
    let (case, geo, history) = case();
    assert!(case.update("user-1", heartbeat(106.8, -6.2)).await.unwrap());

    let stored = geo
        .positions
        .lock()
        .unwrap()
        .get("user-1")
        .cloned()
        .unwrap();
    assert_eq!(stored.longitude, 106.8);
    assert_eq!(stored.latitude, -6.2);
    assert_eq!(history.upserts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_history_failure_does_not_fail_the_heartbeat() {
    let (case, _, history) = case();
    *history.fail.lock().unwrap() = true;
    assert!(case.update("user-1", heartbeat(106.8, -6.2)).await.unwrap());
}

#[tokio::test]
async fn missing_or_out_of_range_coordinates_are_bad_requests() {
    let (case, _, _) = case();

    assert!(matches!(
        case.update(
            "user-1",
            UpdateLocationRequest {
                longitude: Some(106.8),
                latitude: None,
            }
        )
        .await
        .unwrap_err(),
        AppError::BadRequest(_)
    ));

    assert!(matches!(
        case.update("user-1", heartbeat(200.0, -6.2))
            .await
            .unwrap_err(),
        AppError::BadRequest(_)
    ));

    assert!(matches!(
        case.update("user-1", heartbeat(106.8, 89.0))
            .await
            .unwrap_err(),
        AppError::BadRequest(_)
    ));
}

#[tokio::test]
async fn get_reports_fresh_then_stale() {
    let (case, geo, _) = case();
    case.update("user-1", heartbeat(106.8, -6.2)).await.unwrap();

    let status = case.get("user-1").await.unwrap();
    assert!(!status.stale);
    assert_eq!(status.last_seen, NOW);

    geo.last_seen
        .lock()
        .unwrap()
        .insert("user-1".into(), NOW - STALE_THRESHOLD_MS - 1);
    assert!(case.get("user-1").await.unwrap().stale);
}

#[tokio::test]
async fn get_without_a_heartbeat_is_not_found() {
    let (case, _, _) = case();
    assert!(matches!(
        case.get("ghost").await.unwrap_err(),
        AppError::NotFound(_)
    ));
}

#[tokio::test]
async fn nearby_drops_self_stale_unknown_and_masks_the_rest() {
    let (case, geo, _) = case();
    case.update("me", heartbeat(106.8000, -6.2000))
        .await
        .unwrap();
    case.update("fresh", heartbeat(106.8100, -6.2100))
        .await
        .unwrap();
    case.update("stalebie", heartbeat(106.8200, -6.2200))
        .await
        .unwrap();

    geo.last_seen
        .lock()
        .unwrap()
        .insert("stalebie".into(), NOW - STALE_THRESHOLD_MS - 5);
    *geo.hits.lock().unwrap() = vec![
        GeoHit {
            user_id: "me".into(),
            longitude: 106.8,
            latitude: -6.2,
            distance_meters: 0.0,
        },
        GeoHit {
            user_id: "fresh".into(),
            longitude: 106.81234,
            latitude: -6.21234,
            distance_meters: 1234.0,
        },
        GeoHit {
            user_id: "stalebie".into(),
            longitude: 106.82,
            latitude: -6.22,
            distance_meters: 2000.0,
        },
        GeoHit {
            user_id: "ghost-seen".into(),
            longitude: 106.83,
            latitude: -6.23,
            distance_meters: 2500.0,
        },
    ];

    let response = case.list_nearby("me", 0.0).await.unwrap();
    assert_eq!(response.radius_m, 1000.0);
    assert_eq!(response.count, 1);
    assert!(response.masked);
    let responder = &response.responders[0];
    assert_eq!(responder.user_id, "fresh");
    assert_eq!(responder.latitude, -6.212);
    assert_eq!(responder.longitude, 106.812);
    assert_eq!(responder.distance_m, 1200);

    assert_eq!(
        geo.batch_calls.load(Ordering::SeqCst),
        1,
        "last_seen must be one HMGET per candidate set"
    );
}

#[tokio::test]
async fn nearby_requires_a_heartbeat_and_a_sane_radius() {
    let (case, _, _) = case();
    assert!(matches!(
        case.list_nearby("me", 0.0).await.unwrap_err(),
        AppError::NotFound(_)
    ));

    case.update("me", heartbeat(106.8, -6.2)).await.unwrap();
    assert!(matches!(
        case.list_nearby("me", 50.0).await.unwrap_err(),
        AppError::BadRequest(_)
    ));
    assert!(matches!(
        case.list_nearby("me", 6000.0).await.unwrap_err(),
        AppError::BadRequest(_)
    ));
}

#[tokio::test]
async fn nearby_caps_at_fifty_responders() {
    let (case, geo, _) = case();
    case.update("me", heartbeat(106.8, -6.2)).await.unwrap();

    let mut hits = Vec::new();
    for index in 0..60 {
        let user = format!("user-{index}");
        case.update(&user, heartbeat(106.81, -6.21)).await.unwrap();
        hits.push(GeoHit {
            user_id: user,
            longitude: 106.81,
            latitude: -6.21,
            distance_meters: 100.0,
        });
    }
    *geo.hits.lock().unwrap() = hits;

    let response = case.list_nearby("me", 1000.0).await.unwrap();
    assert_eq!(response.count, 50);
}
