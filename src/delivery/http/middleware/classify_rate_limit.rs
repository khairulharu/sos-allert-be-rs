//! Per-user limiter for `POST /api/sos/classify`.
//!
//! Fails **closed** with `503`: every call pays for STT + LLM tokens, so an
//! outage must not become free spend (the SOS trigger limiter fails open for
//! the opposite reason).

use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::bootstrap::AppState;
use crate::error::AppError;
use crate::model::Auth;

pub async fn classify_rate_limit(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let user_id = match request.extensions().get::<Auth>() {
        Some(auth) => auth.id.clone(),
        None => return AppError::Unauthorized.into_response(),
    };

    let limit = state.config.sos.classify_rate_limit_per_hour;
    if limit == 0 {
        return next.run(request).await;
    }

    match state
        .rate_limiter()
        .allow_classify(&user_id, limit, Duration::from_secs(3600))
        .await
    {
        Ok(true) => next.run(request).await,
        Ok(false) => {
            let seconds = 3600_u64;
            let mut response = AppError::TooManyRequests {
                message: format!("classify rate limit exceeded: max {limit} per hour"),
                retry_after_s: seconds,
            }
            .into_response();
            if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
                response.headers_mut().insert("retry-after", value);
            }
            response
        }
        Err(error) => {
            tracing::warn!(error = %error, "classify limiter unavailable, failing closed");
            AppError::Unavailable("classify rate limiter unavailable".into()).into_response()
        }
    }
}
