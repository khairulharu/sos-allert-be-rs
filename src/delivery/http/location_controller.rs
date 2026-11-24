//! Location endpoints.

use axum::extract::rejection::JsonRejection;
use axum::extract::{Json, Query, State};
use axum::Extension;
use serde::Deserialize;
use validator::Validate;

use crate::bootstrap::AppState;
use crate::error::{AppError, AppResult};
use crate::model::{
    Auth, LocationStatusResponse, NearbyLocationResponse, UpdateLocationRequest, WebResponse,
};

use super::user_controller::parse_body;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NearbyQuery {
    pub radius_m: Option<i64>,
}

pub async fn get(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
) -> AppResult<Json<WebResponse<LocationStatusResponse>>> {
    let status = state.locations().get(&auth.id).await?;
    Ok(Json(WebResponse::ok(status)))
}

pub async fn update(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    payload: Result<Json<UpdateLocationRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<bool>>> {
    let request = parse_body(payload)?;
    request
        .validate()
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    let ok = state.locations().update(&auth.id, request).await?;
    Ok(Json(WebResponse::ok(ok)))
}

pub async fn nearby(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Query(query): Query<NearbyQuery>,
) -> AppResult<Json<WebResponse<NearbyLocationResponse>>> {
    let radius = query.radius_m.unwrap_or(0) as f64;
    let response = state.locations().list_nearby(&auth.id, radius).await?;
    Ok(Json(WebResponse::ok(response)))
}
