//! `AuthMiddleware` equivalent: raw `Authorization` token, no `Bearer` prefix.
//!
//! * unknown/expired token -> `401`
//! * session store error -> `503`, so clients keep their token on an outage
//! * the token is never logged

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::bootstrap::AppState;

pub async fn require_auth(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let token = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("NOT_FOUND")
        .to_owned();

    match state.auth().verify(&token).await {
        Ok(auth) => {
            request.extensions_mut().insert(auth);
            next.run(request).await
        }
        Err(error) => error.into_response(),
    }
}
