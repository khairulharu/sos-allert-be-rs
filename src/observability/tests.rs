//! Metrics rendering and token check.

use super::*;

fn value_of(body: &str, name: &str) -> String {
    body.lines()
        .find(|line| line.starts_with(&format!("{name} ")))
        .map(|line| line.split_once(' ').unwrap().1.to_string())
        .unwrap_or_default()
}

#[test]
fn empty_render_has_every_series_at_zero() {
    let body = Metrics::new().render();
    assert!(body.contains("# TYPE sos_dispatch_latency_ms histogram"));
    assert!(body.contains("sos_dispatch_latency_ms_bucket{le=\"50\"} 0"));
    assert!(body.contains("sos_dispatch_latency_ms_bucket{le=\"+Inf\"} 0"));
    assert_eq!(value_of(&body, "sos_dispatch_latency_ms_count"), "0");
    assert_eq!(value_of(&body, "push_delivery_failures_total"), "0");
    assert_eq!(value_of(&body, "sos_fanout_failures_total"), "0");
    assert_eq!(value_of(&body, "active_sos_count"), "0");
}

#[test]
fn buckets_are_cumulative_and_the_sum_is_recorded() {
    let metrics = Metrics::new();
    metrics.observe_dispatch_duration(40.0); // <= 50
    metrics.observe_dispatch_duration(120.0); // <= 150
    metrics.observe_dispatch_duration(900.0); // <= 1000
    metrics.observe_dispatch_duration(5000.0); // +Inf only

    let body = metrics.render();
    assert!(body.contains("sos_dispatch_latency_ms_bucket{le=\"50\"} 1"));
    assert!(body.contains("sos_dispatch_latency_ms_bucket{le=\"100\"} 1"));
    assert!(body.contains("sos_dispatch_latency_ms_bucket{le=\"150\"} 2"));
    assert!(body.contains("sos_dispatch_latency_ms_bucket{le=\"200\"} 2"));
    assert!(body.contains("sos_dispatch_latency_ms_bucket{le=\"500\"} 2"));
    assert!(body.contains("sos_dispatch_latency_ms_bucket{le=\"1000\"} 3"));
    assert!(body.contains("sos_dispatch_latency_ms_bucket{le=\"2000\"} 3"));
    // The observation above the last bound only shows in +Inf and the count.
    assert!(body.contains("sos_dispatch_latency_ms_bucket{le=\"+Inf\"} 4"));
    assert_eq!(value_of(&body, "sos_dispatch_latency_ms_count"), "4");
    assert_eq!(value_of(&body, "sos_dispatch_latency_ms_sum"), "6060");
}

#[test]
fn counters_and_gauge_are_rendered() {
    let metrics = Metrics::new();
    metrics.inc_push_failures();
    metrics.inc_push_failures();
    metrics.inc_fanout_failures();
    metrics.set_active_sos(3);

    let body = metrics.render();
    assert_eq!(value_of(&body, "push_delivery_failures_total"), "2");
    assert_eq!(value_of(&body, "sos_fanout_failures_total"), "1");
    assert_eq!(value_of(&body, "active_sos_count"), "3");
}

#[test]
fn token_check_accepts_raw_and_bearer_and_rejects_the_rest() {
    assert!(token_matches("s3cret", "s3cret"));
    assert!(token_matches("Bearer s3cret", "s3cret"));
    assert!(token_matches("  Bearer s3cret  ", "s3cret"));
    assert!(!token_matches("wrong", "s3cret"));
    assert!(!token_matches("Bearer", "s3cret"));
    assert!(!token_matches("s3cret", ""));
    assert!(!token_matches("", "s3cret"));
}
