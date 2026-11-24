//! Router. Paths, order and auth scoping follow the Go `route.go`.
//!
//! Guest: `POST /api/users` (register), `POST /api/users/_login`, both behind
//! the per-IP auth limiter. Everything else needs the raw token.
//!
//! axum 0.8 path syntax is used (`{contactId}`), which is the same placeholder
//! set Fiber spelled `:contactId`.

use axum::extract::State;
use axum::http::StatusCode;
use axum::middleware as axum_mw;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::json;

use crate::bootstrap::AppState;
use crate::delivery::http::middleware::auth::require_auth;
use crate::delivery::http::middleware::auth_rate_limit::auth_rate_limit;
use crate::delivery::http::middleware::metrics_token::require_metrics_token;
use crate::delivery::http::{
    address_controller, contact_controller, emergency_controller, location_controller,
    user_controller,
};
use crate::repository::health;

pub fn router(state: AppState) -> Router {
    let metrics_token = state.config.metrics.token.clone();

    let guest = Router::new()
        .route("/api/users", post(user_controller::register))
        .route("/api/users/_login", post(user_controller::login))
        .route_layer(axum_mw::from_fn_with_state(state.clone(), auth_rate_limit));

    let authed = Router::new()
        .route(
            "/api/users/_current",
            get(user_controller::current).patch(user_controller::update),
        )
        .route("/api/users", delete(user_controller::logout))
        .route(
            "/api/contacts",
            get(contact_controller::list).post(contact_controller::create),
        )
        .route(
            "/api/contacts/{contactId}",
            get(contact_controller::get)
                .put(contact_controller::update)
                .delete(contact_controller::delete),
        )
        .route(
            "/api/contacts/{contactId}/addresses",
            get(address_controller::list).post(address_controller::create),
        )
        .route(
            "/api/contacts/{contactId}/addresses/{addressId}",
            get(address_controller::get)
                .put(address_controller::update)
                .delete(address_controller::delete),
        )
        .route(
            "/api/emergency-contacts",
            get(emergency_controller::list).post(emergency_controller::create),
        )
        .route("/api/location", get(location_controller::get))
        .route("/api/location/nearby", get(location_controller::nearby))
        .route(
            "/api/location",
            axum::routing::put(location_controller::update),
        )
        .route_layer(axum_mw::from_fn_with_state(state.clone(), require_auth));

    Router::new()
        .route("/healthz", get(liveness))
        .route("/healthz/ready", get(readiness))
        .route("/metrics", get(metrics))
        // Static routes are registered before `:alertId` in the Go service;
        // that ordering matters in phase 3.
        .merge(guest)
        .merge(authed)
        .with_state(state)
        .layer(axum_mw::from_fn(move |req, next| {
            let expected = metrics_token.clone();
            async move { require_metrics_token(expected, req, next).await }
        }))
}

async fn liveness() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

async fn readiness(State(state): State<AppState>) -> Response {
    let db = health::database_ready(state.db()).await;
    let rd = health::redis_ready(state.redis_client()).await;

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
