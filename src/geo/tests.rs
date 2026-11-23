//! Tests for [`super`]. Kept out of `mod.rs` so the logic file stays test-free.

use super::*;

#[test]
fn same_point_is_zero() {
    assert!(distance_meters(-6.1754, 106.8272, -6.1754, 106.8272) < 1e-6);
}

#[test]
fn is_symmetric() {
    let a = (-6.1754, 106.8272); // Jakarta
    let b = (-6.9175, 107.6191); // Bandung
    let ab = distance_meters(a.0, a.1, b.0, b.1);
    let ba = distance_meters(b.0, b.1, a.0, a.1);
    assert!((ab - ba).abs() < 1e-6, "ab={ab} ba={ba}");
}

#[test]
fn jakarta_bandung_is_about_120_km() {
    // 120.5 km measured on the same formula; the band keeps typos out.
    let d = distance_meters(-6.1754, 106.8272, -6.9175, 107.6191);
    assert!((d - 120_500.0).abs() < 3_000.0, "d={d}");
}

#[test]
fn within_one_km_true_below_threshold() {
    // ~111 m per 0.001 degree of latitude.
    assert!(within_one_km(-6.1754, 106.8272, -6.1754, 106.8282));
}

#[test]
fn within_one_km_false_above_threshold() {
    assert!(!within_one_km(-6.1754, 106.8272, -6.1754, 106.8472));
}

#[test]
fn eta_floors_at_sixty_seconds() {
    assert_eq!(estimate_eta_seconds(0.0), MIN_ETA_SECONDS);
    assert_eq!(estimate_eta_seconds(-5.0), MIN_ETA_SECONDS);
    assert_eq!(estimate_eta_seconds(10.0), MIN_ETA_SECONDS);
    assert_eq!(estimate_eta_seconds(84.0), MIN_ETA_SECONDS); // 60 s exactly
}

#[test]
fn eta_matches_walking_speed() {
    // 240 m / 1.4 m/s = 171.4 s -> 171 s.
    assert_eq!(estimate_eta_seconds(240.0), 171);
    // 1 km -> 714.3 s -> 714 s.
    assert_eq!(estimate_eta_seconds(1000.0), 714);
}
