//! Router. Paths, order and auth scoping follow the Go `route.go`.
//!
//! Guest: `POST /api/users` (register), `POST /api/users/_login`, both behind
//! the per-IP auth limiter. Everything else needs the raw token.
//!
//! axum 0.8 path syntax is used (`{contactId}`), which is the same placeholder
//! set Fiber spelled `:contactId`.

use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::middleware as axum_mw;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::json;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::set_header::SetResponseHeaderLayer;

use crate::bootstrap::AppState;
use crate::delivery::http::middleware::auth::require_auth;
use crate::delivery::http::middleware::auth_rate_limit::auth_rate_limit;
use crate::delivery::http::middleware::classify_rate_limit::classify_rate_limit;
use crate::delivery::http::{
    address_controller, contact_controller, emergency_controller, location_controller,
    push_controller, sos_controller, user_controller, ws_track,
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
        // Static SOS routes first; axum prefers them over `{alertId}` anyway.
        .route("/api/sos", post(sos_controller::trigger))
        .route(
            "/api/sos/classify",
            post(sos_controller::classify).layer(axum_mw::from_fn_with_state(
                state.clone(),
                classify_rate_limit,
            )),
        )
        .route("/api/sos/active", get(sos_controller::active))
        .route("/api/sos/inbox", get(sos_controller::inbox))
        .route("/api/sos/history", get(sos_controller::history))
        .route("/api/sos/summary", get(sos_controller::summary))
        .route("/api/sos/{alertId}", get(sos_controller::get))
        .route("/api/sos/{alertId}/cancel", post(sos_controller::cancel))
        .route(
            "/api/sos/{alertId}/acknowledge",
            post(sos_controller::acknowledge),
        )
        .route("/api/sos/{alertId}/resolve", post(sos_controller::resolve))
        .route("/api/sos/{alertId}/respond", post(sos_controller::respond))
        .route(
            "/api/sos/{alertId}/media",
            get(sos_controller::list_media)
                .post(sos_controller::upload_media)
                // Incident uploads are up to 50 MB; axum's default is 2 MB.
                .layer(DefaultBodyLimit::max(50 << 20)),
        )
        .route(
            "/api/sos/{alertId}/media/{mediaId}",
            get(sos_controller::download_media),
        )
        .route(
            "/api/sos/{alertId}/breadcrumbs",
            get(sos_controller::list_breadcrumbs).post(sos_controller::add_breadcrumb),
        )
        .route(
            "/api/push/vapid-public-key",
            get(push_controller::vapid_public_key),
        )
        .route(
            "/api/push/subscriptions",
            get(push_controller::list)
                .post(push_controller::subscribe)
                .delete(push_controller::unsubscribe),
        )
        .route("/api/location", get(location_controller::get))
        .route("/api/location/nearby", get(location_controller::nearby))
        .route(
            "/api/location",
            axum::routing::put(location_controller::update),
        )
        .route_layer(axum_mw::from_fn_with_state(state.clone(), require_auth));

    let root = Router::new()
        .route("/healthz", get(liveness))
        .route("/healthz/ready", get(readiness));

    // `/metrics` is registered only when a token is configured, so the
    // Prometheus body is never public (same rule as the Go service).
    let root = if metrics_token.is_empty() {
        root
    } else {
        root.route("/metrics", get(metrics))
    };

    root
        // Tracking socket: outside the HTTP auth middleware on purpose; the
        // handler does its own in-band auth and connection budgets.
        .route("/ws/sos/{alertId}/track", get(ws_track::handler))
        // Static routes are registered before `:alertId` in the Go service;
        // that ordering matters in phase 3.
        .merge(guest)
        .merge(authed)
        .with_state(state)
        // Parity with the Go Fiber app: a 50 MB body limit, CORS limited to
        // the production site and the Vite dev origin (the browser sends the
        // raw Authorization header and X-Idempotency-Key), the two helmet
        // headers, and a panic guard so one bad handler cannot drop a socket.
        .layer(DefaultBodyLimit::max(50 << 20))
        .layer(cors_layer())
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::X_FRAME_OPTIONS,
            axum::http::HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::HeaderName::from_static("cross-origin-resource-policy"),
            axum::http::HeaderValue::from_static("cross-origin"),
        ))
        .layer(CatchPanicLayer::new())
}

/// Production origin plus the Vite dev server, same allowlist as the Go
/// service. No credentials: sessions travel in the Authorization header.
fn cors_layer() -> CorsLayer {
    use axum::http::header::{ACCEPT, AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_TYPE, ORIGIN};
    use axum::http::Method;

    CorsLayer::new()
        .allow_origin(AllowOrigin::list([
            "https://sos.allert.my.id".parse().expect("origin"),
            "https://allert.my.id".parse().expect("origin"),
            "http://localhost:5173".parse().expect("origin"),
        ]))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            ORIGIN,
            CONTENT_TYPE,
            ACCEPT,
            AUTHORIZATION,
            CONTENT_DISPOSITION,
            axum::http::HeaderName::from_static("x-idempotency-key"),
        ])
        .expose_headers([CONTENT_DISPOSITION])
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

/// Prometheus exposition, guarded by `metrics.token` (raw or `Bearer`).
async fn metrics(State(state): State<AppState>, headers: axum::http::HeaderMap) -> Response {
    let expected = state.config.metrics.token.clone();
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if !crate::observability::token_matches(presented, &expected) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        state.metrics().render(),
    )
        .into_response()
}
