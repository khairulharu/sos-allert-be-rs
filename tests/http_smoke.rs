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
async fn metrics_without_token_is_open() {
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

    assert_eq!(response.status(), StatusCode::OK);
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
async fn metrics_token_guard_accepts_bearer() {
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
