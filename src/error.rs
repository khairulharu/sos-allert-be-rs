//! Error type shared by every layer.
//!
//! The Go original returned sentinel errors (`model.ErrUserNotFound`, ...) that
//! `mapSOSError` translated into HTTP codes. In Rust each layer returns
//! [`AppError`] and the http layer maps it once, in [`AppError::status`].

use thiserror::Error;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    /// Request payload failed validation (`422`).
    #[error("{0}")]
    Validation(String),

    /// Wrong credentials or bad input shape (`400`).
    #[error("{0}")]
    BadRequest(String),

    /// No session for the token (`401`).
    #[error("unauthorized")]
    Unauthorized,

    /// Authenticated but not allowed, e.g. a wrong safe PIN (`403`).
    #[error("{0}")]
    Forbidden(String),

    /// Missing row or resource owned by nobody (`404`).
    #[error("{0}")]
    NotFound(String),

    /// State conflict: duplicate response, idempotency claim (`409`).
    #[error("{0}")]
    Conflict(String),

    /// A missing prerequisite, e.g. no safe PIN configured (`412`).
    #[error("{0}")]
    Precondition(String),

    /// Business rule violation (`422`), mirrors Go's mapSOSError.
    #[error("{0}")]
    Unprocessable(String),

    /// Rate limit / connection limit (`429` + Retry-After).
    #[error("{message}")]
    TooManyRequests { message: String, retry_after_s: u64 },

    /// Storage or upstream failure (`500`), or Redis outage (`503`).
    #[error("{0}")]
    Unavailable(String),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Redis(#[from] redis::RedisError),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl AppError {
    /// HTTP status for this error. `503` is deliberately used for a session
    /// store outage so clients keep their token (same rule as the Go service).
    pub fn status(&self) -> u16 {
        match self {
            AppError::Validation(_) | AppError::Unprocessable(_) => 422,
            AppError::BadRequest(_) => 400,
            AppError::Unauthorized => 401,
            AppError::Forbidden(_) => 403,
            AppError::NotFound(_) => 404,
            AppError::Precondition(_) => 412,
            AppError::Conflict(_) => 409,
            AppError::TooManyRequests { .. } => 429,
            AppError::Unavailable(_) => 503,
            AppError::Sqlx(_) | AppError::Redis(_) | AppError::Json(_) | AppError::Io(_) => 500,
            AppError::Internal(_) => 500,
        }
    }

    /// `Retry-After` value when the error carries one.
    pub fn retry_after_s(&self) -> Option<u64> {
        match self {
            AppError::TooManyRequests { retry_after_s, .. } => Some(*retry_after_s),
            _ => None,
        }
    }
}

impl From<validator::ValidationErrors> for AppError {
    fn from(value: validator::ValidationErrors) -> Self {
        AppError::Validation(value.to_string())
    }
}

/// Render as the documented `{"errors": "..."}` body. Every 5xx is replaced
/// with the generic message and the detail is logged, exactly like the Go
/// `NewErrorHandler`.
impl axum::response::IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        use axum::response::Json;

        let status =
            StatusCode::from_u16(self.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let retry_after = self.retry_after_s();

        let message = if status.is_server_error() {
            tracing::error!(error = %self, "internal error");
            "internal server error".to_string()
        } else {
            self.to_string()
        };

        let mut response = (
            status,
            Json(crate::model::ErrorResponse { errors: message }),
        )
            .into_response();

        if let Some(seconds) = retry_after {
            if let Ok(value) = axum::http::HeaderValue::from_str(&seconds.max(1).to_string()) {
                response.headers_mut().insert("retry-after", value);
            }
        }
        response
    }
}
