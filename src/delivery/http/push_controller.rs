//! Browser Web Push endpoints (`push_controller.go`).

use axum::extract::rejection::JsonRejection;
use axum::extract::{Json, State};
use axum::Extension;

use crate::bootstrap::AppState;
use crate::error::AppResult;
use crate::model::push::{
    PushSubscribeRequest, PushSubscriptionResponse, PushUnsubscribeRequest, PushVapidKeyResponse,
};
use crate::model::{Auth, WebResponse};

use super::user_controller::parse_body;

/// `503` when the channel is not configured.
pub async fn vapid_public_key(
    State(state): State<AppState>,
) -> AppResult<Json<WebResponse<PushVapidKeyResponse>>> {
    let public_key = state.push().vapid_public_key()?;
    Ok(Json(WebResponse::ok(PushVapidKeyResponse { public_key })))
}

pub async fn subscribe(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    payload: Result<Json<PushSubscribeRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<PushSubscriptionResponse>>> {
    let request = parse_body(payload)?;
    let subscription = state.push().subscribe(&auth.id, request).await?;
    Ok(Json(WebResponse::ok(subscription)))
}

pub async fn unsubscribe(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    payload: Result<Json<PushUnsubscribeRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<bool>>> {
    let request = parse_body(payload)?;
    let ok = state.push().unsubscribe(&auth.id, request).await?;
    Ok(Json(WebResponse::ok(ok)))
}

pub async fn list(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
) -> AppResult<Json<WebResponse<Vec<PushSubscriptionResponse>>>> {
    let subs = state.push().list(&auth.id).await?;
    Ok(Json(WebResponse::ok(subs)))
}
