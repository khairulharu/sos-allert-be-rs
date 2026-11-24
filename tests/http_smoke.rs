//! Cross-module smoke tests: the router answers without any live dependency.
//!
//! Unit tests stay inside `src/<module>/tests.rs`; this file only covers wiring.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use sos_allert::bootstrap::build_state;
use sos_allert::config::Config;
use sos_allert::delivery::http::routes::router;
use tower::ServiceExt;

fn test_state() -> sos_allert::bootstrap::AppState {
    // Defaults are valid and point at localhost; nothing is dialled until a
    // request needs it.
    let config = Config::default();
    build_state(config).expect("state builds from defaults")
}

#[tokio::test]
async fn healthz_is_ok() {
    let app = router(test_state());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn metrics_is_not_registered_without_a_token() {
    // Parity with the Go service: the route stays unregistered unless
    // `metrics.token` is configured, so the body is never public.
    let app = router(test_state());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn metrics_token_guard_rejects_missing_bearer() {
    let mut config = Config::default();
    config.metrics.token = "s3cret".into();
    let app = router(build_state(config).unwrap());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn metrics_token_guard_accepts_bearer_and_renders_prometheus_text() {
    let mut config = Config::default();
    config.metrics.token = "s3cret".into();
    let app = router(build_state(config).unwrap());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", "Bearer s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("sos_dispatch_latency_ms_count 0"), "{text}");
    assert!(text.contains("active_sos_count 0"), "{text}");
    assert!(text.contains("push_delivery_failures_total 0"), "{text}");
}

#[tokio::test]
async fn metrics_token_guard_accepts_the_raw_token() {
    let mut config = Config::default();
    config.metrics.token = "s3cret".into();
    let app = router(build_state(config).unwrap());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", "s3cret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn readiness_reports_unreachable_dependencies() {
    // Nothing listens on these ports in the test environment, so readiness must
    // answer 503 with a reason instead of panicking or hanging.
    let mut config = Config::default();
    config.database.host = "127.0.0.1".into();
    config.database.port = 1;
    config.redis.addr = "127.0.0.1:1".into();
    let app = router(build_state(config).unwrap());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn cors_allows_only_the_configured_origins() {
    let app = router(test_state());

    let allowed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .header("origin", "https://allert.my.id")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        allowed
            .headers()
            .get("access-control-allow-origin")
            .and_then(|value| value.to_str().ok()),
        Some("https://allert.my.id")
    );

    let dev = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .header("origin", "http://localhost:5173")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        dev.headers()
            .get("access-control-allow-origin")
            .and_then(|value| value.to_str().ok()),
        Some("http://localhost:5173")
    );

    let foreign = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .header("origin", "https://evil.example")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        foreign
            .headers()
            .get("access-control-allow-origin")
            .is_none(),
        "a foreign origin must not be allowed"
    );
}

#[tokio::test]
async fn preflight_allows_the_raw_authorization_and_idempotency_headers() {
    let app = router(test_state());
    let response = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/sos")
                .header("origin", "https://allert.my.id")
                .header("access-control-request-method", "POST")
                .header(
                    "access-control-request-headers",
                    "authorization,x-idempotency-key,content-type",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status().is_success(), "preflight rejected");
    let allowed = response
        .headers()
        .get("access-control-allow-headers")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(allowed.contains("authorization"), "{allowed}");
    assert!(allowed.contains("x-idempotency-key"), "{allowed}");
}

#[tokio::test]
async fn helmet_headers_are_present() {
    let app = router(test_state());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response
            .headers()
            .get("x-frame-options")
            .and_then(|value| value.to_str().ok()),
        Some("DENY")
    );
    assert_eq!(
        response
            .headers()
            .get("cross-origin-resource-policy")
            .and_then(|value| value.to_str().ok()),
        Some("cross-origin")
    );
}
