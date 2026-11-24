//! Address endpoints (nested under a contact).

use axum::extract::rejection::JsonRejection;
use axum::extract::{Json, Path, State};
use axum::Extension;

use crate::bootstrap::AppState;
use crate::error::AppResult;
use crate::model::{
    AddressResponse, Auth, CreateAddressRequest, UpdateAddressRequest, WebResponse,
};

use super::user_controller::parse_body;

pub async fn create(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(contact_id): Path<String>,
    payload: Result<Json<CreateAddressRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<AddressResponse>>> {
    let request = parse_body(payload)?;
    let address = state
        .addresses()
        .create(&auth.id, &contact_id, request)
        .await?;
    Ok(Json(WebResponse::ok(address)))
}

pub async fn list(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(contact_id): Path<String>,
) -> AppResult<Json<WebResponse<Vec<AddressResponse>>>> {
    let addresses = state.addresses().list(&auth.id, &contact_id).await?;
    Ok(Json(WebResponse::ok(addresses)))
}

pub async fn get(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path((contact_id, address_id)): Path<(String, String)>,
) -> AppResult<Json<WebResponse<AddressResponse>>> {
    let address = state
        .addresses()
        .get(&auth.id, &contact_id, &address_id)
        .await?;
    Ok(Json(WebResponse::ok(address)))
}

pub async fn update(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path((contact_id, address_id)): Path<(String, String)>,
    payload: Result<Json<UpdateAddressRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<AddressResponse>>> {
    let request = parse_body(payload)?;
    let address = state
        .addresses()
        .update(&auth.id, &contact_id, &address_id, request)
        .await?;
    Ok(Json(WebResponse::ok(address)))
}

pub async fn delete(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path((contact_id, address_id)): Path<(String, String)>,
) -> AppResult<Json<WebResponse<bool>>> {
    state
        .addresses()
        .delete(&auth.id, &contact_id, &address_id)
        .await?;
    Ok(Json(WebResponse::ok(true)))
}
