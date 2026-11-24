//! Location API models.

use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocationResponse {
    pub id: String,
    pub user_id: String,
    pub longitude: f64,
    pub latitude: f64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// `PUT /api/location`. Coordinates are optional pointers in Go
/// (`*float64` + required), so a missing key is a validation error, not 0,0.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct UpdateLocationRequest {
    #[validate(range(min = -180.0, max = 180.0))]
    pub longitude: Option<f64>,
    /// Latitude bound is ±85.05, the Web Mercator limit.
    #[validate(range(min = -85.05, max = 85.05))]
    pub latitude: Option<f64>,
}

/// The authenticated user's last heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationStatusResponse {
    pub user_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub last_seen: i64,
    /// Mirrors the >2 min filter used for SOS fan-out.
    pub stale: bool,
}

/// One active user inside the requested radius. Coordinates are masked to
/// ~100 m, and the distance is snapped to a 100 m bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NearbyResponder {
    pub user_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub distance_m: i64,
    pub last_seen: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NearbyLocationResponse {
    pub radius_m: f64,
    pub count: usize,
    pub responders: Vec<NearbyResponder>,
    pub masked: bool,
}
