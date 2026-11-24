//! Location use case (`location_usecase.go`).
//!
//! Redis GEO (`users:locations` + `users:last_seen`) is the operational store
//! for fan-out; Postgres keeps one historical row per user and must never fail
//! a heartbeat.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use validator::Validate;

use crate::domain::LocationCache;
use crate::error::{AppError, AppResult};
use crate::geo::{mask_coordinates, quantize_distance_meters};
use crate::model::{
    LocationStatusResponse, NearbyLocationResponse, NearbyResponder, UpdateLocationRequest,
};

/// Stale after 2 minutes, the same cutoff the SOS fan-out uses.
pub const STALE_THRESHOLD_MS: i64 = 2 * 60 * 1000;
pub const DEFAULT_NEARBY_RADIUS_METERS: f64 = 1000.0;
pub const MIN_NEARBY_RADIUS_METERS: f64 = 100.0;
pub const MAX_NEARBY_RADIUS_METERS: f64 = 5000.0;
pub const MAX_NEARBY_RESPONDERS: usize = 50;
pub const NEARBY_SEARCH_COUNT: usize = 100;

/// One GEO search hit with its ground distance.
#[derive(Debug, Clone, PartialEq)]
pub struct GeoHit {
    pub user_id: String,
    pub longitude: f64,
    pub latitude: f64,
    pub distance_meters: f64,
}

#[async_trait]
pub trait LocationGeoStore: Send + Sync {
    /// GEOADD + `last_seen` write.
    async fn save(&self, location: &LocationCache, now_ms: i64) -> AppResult<()>;
    async fn get(&self, user_id: &str) -> AppResult<Option<LocationCache>>;
    async fn last_seen(&self, user_id: &str) -> AppResult<Option<i64>>;
    /// One `HMGET` for the whole candidate set, never one call per candidate.
    async fn last_seen_batch(&self, user_ids: &[String]) -> AppResult<HashMap<String, i64>>;
    async fn search_with_distance(
        &self,
        longitude: f64,
        latitude: f64,
        radius_meters: f64,
        count: usize,
    ) -> AppResult<Vec<GeoHit>>;
}

#[async_trait]
pub trait LocationHistoryStore: Send + Sync {
    async fn upsert(
        &self,
        user_id: &str,
        longitude: f64,
        latitude: f64,
        now_ms: i64,
    ) -> AppResult<()>;
}

pub struct LocationUseCase {
    geo: Arc<dyn LocationGeoStore>,
    history: Option<Arc<dyn LocationHistoryStore>>,
    now_millis: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl LocationUseCase {
    pub fn new(
        geo: Arc<dyn LocationGeoStore>,
        history: Option<Arc<dyn LocationHistoryStore>>,
    ) -> Self {
        Self {
            geo,
            history,
            now_millis: Arc::new(crate::usecase::auth::default_now_millis),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.now_millis = clock;
        self
    }

    pub async fn update(&self, user_id: &str, request: UpdateLocationRequest) -> AppResult<bool> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;

        let (longitude, latitude) = match (request.longitude, request.latitude) {
            (Some(longitude), Some(latitude)) => (longitude, latitude),
            _ => {
                return Err(AppError::BadRequest(
                    "longitude and latitude are required".into(),
                ))
            }
        };

        let now = (self.now_millis)();
        self.geo
            .save(&LocationCache::new(user_id, longitude, latitude), now)
            .await?;

        // History is best effort: a Postgres hiccup must not fail the heartbeat.
        if let Some(history) = &self.history {
            if let Err(e) = history.upsert(user_id, longitude, latitude, now).await {
                tracing::warn!(error = %e, user_id, "location history upsert failed");
            }
        }

        Ok(true)
    }

    pub async fn get(&self, user_id: &str) -> AppResult<LocationStatusResponse> {
        let location = self
            .geo
            .get(user_id)
            .await?
            .ok_or_else(|| AppError::NotFound("user location not found".into()))?;

        // A missing timestamp is unknown, not a failed read.
        let last_seen = match self.geo.last_seen(user_id).await {
            Ok(Some(value)) => value,
            Ok(None) => 0,
            Err(e) => {
                tracing::debug!(error = %e, user_id, "no last_seen timestamp");
                0
            }
        };

        let now = (self.now_millis)();
        Ok(LocationStatusResponse {
            user_id: location.user_id,
            latitude: location.latitude,
            longitude: location.longitude,
            last_seen,
            stale: last_seen == 0 || now - last_seen > STALE_THRESHOLD_MS,
        })
    }

    /// Responder map around the caller. The caller must have a heartbeat,
    /// stale users are dropped, and coordinates are masked to ~100 m.
    pub async fn list_nearby(
        &self,
        user_id: &str,
        radius_meters: f64,
    ) -> AppResult<NearbyLocationResponse> {
        let radius = if radius_meters == 0.0 {
            DEFAULT_NEARBY_RADIUS_METERS
        } else {
            radius_meters
        };
        if !(MIN_NEARBY_RADIUS_METERS..=MAX_NEARBY_RADIUS_METERS).contains(&radius) {
            return Err(AppError::BadRequest(
                "radius_m must be between 100 and 5000".into(),
            ));
        }

        let own = self
            .geo
            .get(user_id)
            .await?
            .ok_or_else(|| AppError::NotFound("user location not found".into()))?;

        let hits = self
            .geo
            .search_with_distance(own.longitude, own.latitude, radius, NEARBY_SEARCH_COUNT)
            .await?;

        let candidates: Vec<String> = hits
            .iter()
            .filter(|hit| hit.user_id != user_id)
            .map(|hit| hit.user_id.clone())
            .collect();

        // A batch failure drops candidates whose freshness is unknown, which
        // is the same degradation as the per-user skip in the Go service.
        let last_seen_by_user = match self.geo.last_seen_batch(&candidates).await {
            Ok(map) => map,
            Err(e) => {
                tracing::warn!(error = %e, "last_seen batch failed");
                HashMap::new()
            }
        };

        let now = (self.now_millis)();
        let mut responders = Vec::with_capacity(hits.len());
        for hit in &hits {
            if hit.user_id == user_id {
                continue;
            }
            let Some(last_seen) = last_seen_by_user.get(&hit.user_id).copied() else {
                continue;
            };
            if last_seen == 0 || now - last_seen > STALE_THRESHOLD_MS {
                continue;
            }

            let (latitude, longitude) = mask_coordinates(hit.latitude, hit.longitude);
            responders.push(NearbyResponder {
                user_id: hit.user_id.clone(),
                latitude,
                longitude,
                distance_m: quantize_distance_meters(hit.distance_meters),
                last_seen,
            });
            if responders.len() >= MAX_NEARBY_RESPONDERS {
                break;
            }
        }

        Ok(NearbyLocationResponse {
            radius_m: radius,
            count: responders.len(),
            responders,
            masked: true,
        })
    }
}
