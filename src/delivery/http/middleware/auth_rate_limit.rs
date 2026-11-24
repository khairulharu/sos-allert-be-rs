//! Per-IP auth limiter for the guest routes.
//!
//! Fails **open**: a Redis outage must never lock people out of authentication
//! (`NewAuthRateLimit` in the Go service). Blocked attempts get `429` plus
//! `Retry-After` in seconds, floored at 1.

use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::bootstrap::AppState;
use crate::error::AppError;

/// Client IP: `web.proxy_header` when configured, else the socket address,
/// else `unknown`. Only the first hop is trusted, matching `ctx.IP()`.
pub fn client_ip(request: &Request, proxy_header: &str) -> String {
    if !proxy_header.is_empty() {
        if let Some(value) = request
            .headers()
            .get(proxy_header)
            .and_then(|value| value.to_str().ok())
        {
            if let Some(first) = value.split(',').next() {
                let trimmed = first.trim();
                if !trimmed.is_empty() {
                    return trimmed.to_owned();
                }
            }
        }
    }

    request
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|info| info.0.ip().to_string())
        .unwrap_or_else(|| "unknown".to_owned())
}

pub async fn auth_rate_limit(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let config = state.config.clone();
    let limit = config.auth.rate_limit_per_minute;
    let lockout = Duration::from_secs(config.auth.lockout_minutes * 60);

    if limit == 0 || lockout.is_zero() {
        return next.run(request).await;
    }

    let ip = client_ip(&request, &config.web.proxy_header);
    match state
        .rate_limiter()
        .allow_auth(&ip, limit, Duration::from_secs(60), lockout)
        .await
    {
        Ok((true, _)) => next.run(request).await,
        Ok((false, retry_after)) => {
            let seconds = retry_after.as_secs().max(1);
            let mut response = AppError::TooManyRequests {
                message: "too many authentication attempts, try again later".into(),
                retry_after_s: seconds,
            }
            .into_response();
            if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
                response.headers_mut().insert("retry-after", value);
            }
            response
        }
        Err(error) => {
            tracing::warn!(error = %error, "auth rate limiter unavailable, proceeding");
            next.run(request).await
        }
    }
}
