//! Contact endpoints.

use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Json, Path, Query, State};
use axum::Extension;

use crate::bootstrap::AppState;
use crate::error::{AppError, AppResult};
use crate::model::{
    Auth, ContactResponse, CreateContactRequest, PageMetadata, SearchContactRequest,
    UpdateContactRequest, WebResponse,
};

use super::user_controller::parse_body;

fn parse_query<T>(query: Result<Query<T>, QueryRejection>) -> AppResult<T> {
    query
        .map(|Query(value)| value)
        .map_err(|_| AppError::BadRequest("Bad Request".into()))
}

pub async fn create(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    payload: Result<Json<CreateContactRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<ContactResponse>>> {
    let request = parse_body(payload)?;
    let contact = state.contacts().create(&auth.id, request).await?;
    Ok(Json(WebResponse::ok(contact)))
}

pub async fn list(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    query: Result<Query<SearchContactRequest>, QueryRejection>,
) -> AppResult<Json<WebResponse<Vec<ContactResponse>>>> {
    let request = parse_query(query)?;
    let page = request.page;
    let (rows, total, total_page, size) = state.contacts().search(&auth.id, request).await?;

    Ok(Json(WebResponse {
        data: rows,
        paging: Some(PageMetadata {
            page,
            size,
            total_item: total,
            total_page,
        }),
        errors: None,
    }))
}

pub async fn get(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(contact_id): Path<String>,
) -> AppResult<Json<WebResponse<ContactResponse>>> {
    let contact = state.contacts().get(&auth.id, &contact_id).await?;
    Ok(Json(WebResponse::ok(contact)))
}

pub async fn update(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(contact_id): Path<String>,
    payload: Result<Json<UpdateContactRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<ContactResponse>>> {
    let request = parse_body(payload)?;
    let contact = state
        .contacts()
        .update(&auth.id, &contact_id, request)
        .await?;
    Ok(Json(WebResponse::ok(contact)))
}

pub async fn delete(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(contact_id): Path<String>,
) -> AppResult<Json<WebResponse<bool>>> {
    state.contacts().delete(&auth.id, &contact_id).await?;
    Ok(Json(WebResponse::ok(true)))
}
