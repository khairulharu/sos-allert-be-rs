//! Emergency contact endpoints (`/api/emergency-contacts`).

use axum::extract::rejection::JsonRejection;
use axum::extract::{Json, State};
use axum::Extension;

use crate::bootstrap::AppState;
use crate::error::AppResult;
use crate::model::{Auth, WebResponse};
use crate::usecase::emergency::{CreateEmergencyContactRequest, EmergencyContactResponse};

use super::user_controller::parse_body;

pub async fn list(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
) -> AppResult<Json<WebResponse<Vec<EmergencyContactResponse>>>> {
    let contacts = state.emergency_contacts().list(&auth.id).await?;
    Ok(Json(WebResponse::ok(contacts)))
}

pub async fn create(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    payload: Result<Json<CreateEmergencyContactRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<EmergencyContactResponse>>> {
    let request = parse_body(payload)?;
    let contact = state.emergency_contacts().create(&auth.id, request).await?;
    Ok(Json(WebResponse::ok(contact)))
}
