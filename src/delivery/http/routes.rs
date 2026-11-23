//! Router. Route paths, order and auth scoping follow the Go `route.go`
//! exactly; only the health and metrics routes exist so far.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;

use crate::bootstrap::AppState;
use crate::delivery::http::middleware::metrics_token::require_metrics_token;
use crate::repository::health;

pub fn router(state: AppState) -> Router {
    let metrics_token = state.config.metrics.token.clone();

    Router::new()
        .route("/healthz", get(liveness))
        .route("/healthz/ready", get(readiness))
        .route("/metrics", get(metrics))
        // Static and ws routes are registered before the auth middleware in the
        // Go service (`/api/sos/history`, `/ws/...`); phase 1 adds them here.
        .with_state(state)
        .layer(axum::middleware::from_fn(move |req, next| {
            let expected = metrics_token.clone();
            async move { require_metrics_token(expected, req, next).await }
        }))
}

async fn liveness() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

async fn readiness(State(state): State<AppState>) -> Response {
    let db = health::database_ready(state.db()).await;
    let rd = health::redis_ready(state.redis()).await;

    let ok = db.is_ok() && rd.is_ok();
    let body = json!({
        "status": if ok { "ready" } else { "degraded" },
        "postgres": db.err(),
        "redis": rd.err(),
    });

    let status = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(body)).into_response()
}

async fn metrics() -> impl IntoResponse {
    // Wired in phase 3 with `metrics` + `metrics-exporter-prometheus`.
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        "# metrics endpoint wired in phase 3\n",
    )
}
