//! Distance and ETA helpers.
//!
//! Ported 1:1 from the Go service (`internal/usecase/haversine_calculate.go`)
//! so responder distance/ETA stay numerically identical after the migration.

/// Mean earth radius used by the Go service.
pub const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

/// Assumed responder travel speed (1.4 m/s, ~5 km/h walking).
pub const WALKING_SPEED_MPS: f64 = 1.4;

/// Minimum ETA reported to a sender, in seconds.
pub const MIN_ETA_SECONDS: u64 = 60;

/// Great-circle distance in meters between two WGS84 points.
pub fn distance_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let d_lat = to_rad(lat2 - lat1);
    let d_lon = to_rad(lon2 - lon1);
    let a = (d_lat / 2.0).sin().powi(2)
        + to_rad(lat1).cos() * to_rad(lat2).cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_METERS * c
}

/// Legacy predicate kept for parity: are the two points closer than 1 km?
pub fn within_one_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> bool {
    distance_meters(lat1, lon1, lat2, lon2) < 1000.0
}

/// Walking ETA in seconds, never below [`MIN_ETA_SECONDS`].
pub fn estimate_eta_seconds(distance_meters: f64) -> u64 {
    if distance_meters <= 0.0 {
        return MIN_ETA_SECONDS;
    }
    let eta = (distance_meters / WALKING_SPEED_MPS + 0.5) as u64;
    eta.max(MIN_ETA_SECONDS)
}

fn to_rad(deg: f64) -> f64 {
    (deg * std::f64::consts::PI) / 180.0
}

/// Round a position to ~100 m (3 decimal degrees) so an unconfirmed crowd
/// responder never sees a victim's exact address.
pub fn mask_coordinates(latitude: f64, longitude: f64) -> (f64, f64) {
    (round3(latitude), round3(longitude))
}

fn round3(value: f64) -> f64 {
    const FACTOR: f64 = 1000.0;
    if value >= 0.0 {
        (value * FACTOR + 0.5).trunc() / FACTOR
    } else {
        -((-value * FACTOR + 0.5).trunc()) / FACTOR
    }
}

/// Snap a distance to its nearest 100 m bucket, so the label stays usable
/// while two bearings from two vantages no longer intersect at a point.
pub fn quantize_distance_meters(meters: f64) -> i64 {
    (((meters + 50.0) / 100.0) as i64) * 100
}

#[cfg(test)]
mod tests;
