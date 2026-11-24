//! SOS endpoints.

use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Json, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use serde::Deserialize;

use crate::bootstrap::AppState;
use crate::error::{AppError, AppResult};
use crate::model::sos::{
    AlertDetailResponse, AlertSummaryResponse, BreadcrumbRequest, BreadcrumbResponse,
    CancelSosRequest, CancelSosResponse, RespondSosRequest, RespondSosResponse,
    SearchAlertHistoryRequest, TriggerSosRequest,
};
use crate::model::{Auth, PageMetadata, WebResponse};

use super::user_controller::parse_body;

fn parse_query<T>(query: Result<Query<T>, QueryRejection>) -> AppResult<T> {
    query
        .map(|Query(value)| value)
        .map_err(|_| AppError::BadRequest("Bad Request".into()))
}

/// `POST /api/sos` - 201 with the alert id and fan-out size.
pub async fn trigger(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    headers: HeaderMap,
    payload: Result<Json<TriggerSosRequest>, JsonRejection>,
) -> AppResult<Response> {
    // An empty body is valid: the last heartbeat is used instead.
    let request = match payload {
        Ok(Json(value)) => value,
        Err(rejection) if rejection.body_text().contains("EOF") => TriggerSosRequest::default(),
        Err(_) => return Err(AppError::BadRequest("Bad Request".into())),
    };

    let idempotency_key = headers
        .get("x-idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    let response = state
        .sos()
        .trigger(&auth.id, request, idempotency_key.as_deref())
        .await?;

    Ok((StatusCode::CREATED, Json(WebResponse::ok(response))).into_response())
}

pub async fn active(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
) -> AppResult<Json<WebResponse<Vec<AlertDetailResponse>>>> {
    let alerts = state.sos().list_active(&auth.id).await?;
    Ok(Json(WebResponse::ok(alerts)))
}

pub async fn inbox(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
) -> AppResult<Json<WebResponse<Vec<AlertDetailResponse>>>> {
    let alerts = state.sos().list_inbox(&auth.id).await?;
    Ok(Json(WebResponse::ok(alerts)))
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct HistoryQuery {
    pub role: Option<String>,
    pub status: Option<String>,
    pub page: Option<i64>,
    pub size: Option<i64>,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
}

pub async fn history(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    query: Result<Query<HistoryQuery>, QueryRejection>,
) -> AppResult<Json<WebResponse<Vec<AlertDetailResponse>>>> {
    let query = parse_query(query)?;
    let request = SearchAlertHistoryRequest {
        user_id: auth.id,
        role: query.role.unwrap_or_else(|| "sent".into()),
        status: query.status.unwrap_or_default(),
        page: query.page.unwrap_or(1),
        size: query.size.unwrap_or(20),
        from_ms: query.from_ms,
        to_ms: query.to_ms,
    };
    let page = request.page;
    let size = request.size;

    let (rows, total) = state.sos().history(request).await?;
    let total_page = if size > 0 {
        (total + size - 1) / size
    } else {
        0
    };

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

pub async fn summary(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
) -> AppResult<Json<WebResponse<AlertSummaryResponse>>> {
    let summary = state.sos().summary(&auth.id).await?;
    Ok(Json(WebResponse::ok(summary)))
}

pub async fn get(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(alert_id): Path<String>,
) -> AppResult<Json<WebResponse<AlertDetailResponse>>> {
    let detail = state.sos().get_alert(&auth.id, &alert_id).await?;
    Ok(Json(WebResponse::ok(detail)))
}

pub async fn cancel(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(alert_id): Path<String>,
    payload: Result<Json<CancelSosRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<CancelSosResponse>>> {
    let request = parse_body(payload)?;
    let response = state.sos().cancel(&auth.id, &alert_id, request).await?;
    Ok(Json(WebResponse::ok(response)))
}

pub async fn acknowledge(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(alert_id): Path<String>,
) -> AppResult<Json<WebResponse<AlertDetailResponse>>> {
    let detail = state.sos().acknowledge(&auth.id, &alert_id).await?;
    Ok(Json(WebResponse::ok(detail)))
}

pub async fn resolve(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(alert_id): Path<String>,
) -> AppResult<Json<WebResponse<AlertDetailResponse>>> {
    let detail = state.sos().resolve(&auth.id, &alert_id).await?;
    Ok(Json(WebResponse::ok(detail)))
}

pub async fn respond(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(alert_id): Path<String>,
    payload: Result<Json<RespondSosRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<RespondSosResponse>>> {
    let request = parse_body(payload)?;
    let response = state.sos().respond(&auth.id, &alert_id, request).await?;
    Ok(Json(WebResponse::ok(response)))
}

pub async fn add_breadcrumb(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(alert_id): Path<String>,
    payload: Result<Json<BreadcrumbRequest>, JsonRejection>,
) -> AppResult<Json<WebResponse<BreadcrumbResponse>>> {
    let request = parse_body(payload)?;
    let response = state
        .sos()
        .add_breadcrumb(&auth.id, &alert_id, request)
        .await?;
    Ok(Json(WebResponse::ok(response)))
}

pub async fn list_breadcrumbs(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(alert_id): Path<String>,
) -> AppResult<Json<WebResponse<Vec<BreadcrumbResponse>>>> {
    let crumbs = state.sos().list_breadcrumbs(&auth.id, &alert_id).await?;
    Ok(Json(WebResponse::ok(crumbs)))
}
