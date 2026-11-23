//! `/metrics` guard: only active when `metrics.token` is set (parity with the
//! Go service, where an empty token leaves the endpoint open).

use axum::extract::Request;
use axum::http::header::AUTHORIZATION;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

pub async fn require_metrics_token(expected: String, req: Request, next: Next) -> Response {
    if expected.is_empty() || req.uri().path() != "/metrics" {
        return next.run(req).await;
    }

    let presented = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_owned);

    match presented {
        Some(token) if token == expected => next.run(req).await,
        _ => (StatusCode::UNAUTHORIZED, "unauthorized metrics access").into_response(),
    }
}
