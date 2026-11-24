//! User endpoints: register, login, current, logout, update.

use axum::extract::rejection::JsonRejection;
use axum::extract::{Json, State};
use axum::Extension;

use crate::bootstrap::AppState;
use crate::error::{AppError, AppResult};
use crate::model::{
    Auth, LoginUserRequest, RegisterUserRequest, UpdateUserRequest, UserResponse, WebResponse,
};

/// A malformed body is `400 Bad Request` with the documented error shape, not
/// axum's default rejection body.
pub fn parse_body<T>(payload: Result<Json<T>, JsonRejection>) -> AppResult<T> {
    payload
        .map(|Json(value)| value)
        .map_err(|_| AppError::BadRequest("Bad Request".into()))
}

pub async fn register(
    State(state): State<AppState>,
    payload: Result<Json<RegisterUserRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<UserResponse>>> {
    let request = parse_body(payload)?;
    let response = state.auth().create(request).await?;
    Ok(Json(WebResponse::ok(response)))
}

pub async fn login(
    State(state): State<AppState>,
    payload: Result<Json<LoginUserRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<UserResponse>>> {
    let request = parse_body(payload)?;
    let response = state.auth().login(request).await?;
    Ok(Json(WebResponse::ok(response)))
}

pub async fn current(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
) -> AppResult<Json<WebResponse<UserResponse>>> {
    let response = state.auth().current(&auth.id).await?;
    Ok(Json(WebResponse::ok(response)))
}

pub async fn logout(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
) -> AppResult<Json<WebResponse<bool>>> {
    let response = state.auth().logout(&auth.id).await?;
    Ok(Json(WebResponse::ok(response)))
}

pub async fn update(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    payload: Result<Json<UpdateUserRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<UserResponse>>> {
    let mut request = parse_body(payload)?;
    request.id = auth.id;
    let response = state.auth().update(request).await?;
    Ok(Json(WebResponse::ok(response)))
}
