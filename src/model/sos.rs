//! SOS API models (`internal/model/sos_model.go`).

use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::geo::{distance_meters, estimate_eta_seconds};

#[derive(Debug, Clone, Default, Deserialize, Validate)]
pub struct TriggerSosRequest {
    /// Optional: when omitted the sender's last Redis GEO position is used.
    #[validate(range(min = -85.05, max = 85.05))]
    #[serde(default)]
    pub latitude: Option<f64>,
    #[validate(range(min = -180.0, max = 180.0))]
    #[serde(default)]
    pub longitude: Option<f64>,
    #[validate(range(min = 0.0))]
    #[serde(default)]
    pub accuracy_meters: Option<f64>,
    #[validate(range(min = 0, max = 100))]
    #[serde(default)]
    pub battery_percentage: Option<i32>,
    #[validate(custom(function = "validate_emergency_type"))]
    #[serde(default)]
    pub emergency_type: String,
    #[validate(length(max = 1000))]
    #[serde(default)]
    pub description: String,
    /// The sender's own claim. Never drives triage or the fan-out radius.
    #[validate(range(min = 0.0, max = 1.0))]
    #[serde(default)]
    pub client_severity: Option<f64>,
    #[validate(length(max = 500))]
    #[serde(default)]
    pub client_summary: String,
}

fn validate_emergency_type(value: &str) -> Result<(), validator::ValidationError> {
    match value {
        "" | "DANGER" | "MEDICAL" | "ACCIDENT" => Ok(()),
        _ => Err(validator::ValidationError::new("emergency_type")),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct TriageInfo {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub emergency_type: String,
    pub severity: f64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SosCreatedResponse {
    pub alert_id: String,
    pub status: String,
    pub grace_period_seconds: u64,
    pub recipients_notified: usize,
    pub triggered_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triage: Option<TriageInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_severity: Option<f64>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub client_summary: String,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct CancelSosRequest {
    #[validate(custom(function = "validate_pin"))]
    pub pin: String,
    #[validate(custom(function = "validate_reason"))]
    #[serde(default)]
    pub reason: String,
}

fn validate_pin(value: &str) -> Result<(), validator::ValidationError> {
    if value.len() == 4 && value.chars().all(|c| c.is_ascii_digit()) {
        Ok(())
    } else {
        Err(validator::ValidationError::new("pin"))
    }
}

fn validate_reason(value: &str) -> Result<(), validator::ValidationError> {
    match value {
        "" | "FALSE_ALARM" | "ACCIDENTAL" | "SAFE_NOW" | "TEST" => Ok(()),
        _ => Err(validator::ValidationError::new("reason")),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelSosResponse {
    pub alert_id: String,
    pub status: String,
    pub cancelled_at: i64,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RespondSosRequest {
    #[validate(range(min = -85.05, max = 85.05))]
    pub responder_latitude: Option<f64>,
    #[validate(range(min = -180.0, max = 180.0))]
    pub responder_longitude: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RespondSosResponse {
    pub response_id: String,
    pub eta_seconds: i64,
    pub victim_distance_meters: i64,
}

#[derive(Debug, Clone, Deserialize, Validate, Default)]
pub struct BreadcrumbRequest {
    #[validate(range(min = -85.05, max = 85.05))]
    pub latitude: Option<f64>,
    #[validate(range(min = -180.0, max = 180.0))]
    pub longitude: Option<f64>,
    #[serde(default)]
    #[validate(range(min = 0.0))]
    pub speed_mps: Option<f64>,
    #[serde(default)]
    #[validate(range(min = 0.0, max = 360.0))]
    pub heading_deg: Option<f64>,
    #[serde(default)]
    pub altitude_meters: Option<f64>,
    #[serde(default)]
    #[validate(range(min = 0.0))]
    pub accuracy_meters: Option<f64>,
    #[serde(default)]
    #[validate(range(min = 0, max = 100))]
    pub battery_percentage: Option<i32>,
    /// Client timestamp in ms. Older than 24 h is a `400`, more than 60 s in
    /// the future is clamped to now (ordering-sensitive).
    #[serde(default)]
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BreadcrumbResponse {
    pub latitude: f64,
    pub longitude: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed_mps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heading_deg: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub altitude_meters: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accuracy_meters: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub battery_percentage: Option<i32>,
    pub recorded_at: i64,
}

/// Everything a notification needs, including the sender's own claim.
/// Kept separate from the entity: the claim is never stored, it only labels
/// the push ("Pelapor menyebut: ...").
#[derive(Debug, Clone, PartialEq)]
pub struct AlertNotice {
    pub alert_id: String,
    pub sender_id: String,
    pub status: String,
    pub emergency_type: String,
    pub severity: Option<f64>,
    pub summary: String,
    pub client_severity: Option<f64>,
    pub client_summary: String,
    pub latitude: f64,
    pub longitude: f64,
    pub triggered_at: i64,
}

impl From<&crate::domain::Alert> for AlertNotice {
    fn from(alert: &crate::domain::Alert) -> Self {
        Self {
            alert_id: alert.id.clone(),
            sender_id: alert.sender_id.clone(),
            status: alert.status.clone(),
            emergency_type: alert.emergency_type.clone(),
            severity: alert.severity,
            summary: alert.summary.clone().unwrap_or_default(),
            client_severity: None,
            client_summary: String::new(),
            latitude: alert.latitude,
            longitude: alert.longitude,
            triggered_at: alert.triggered_at,
        }
    }
}

impl AlertNotice {
    pub fn with_client_claim(mut self, severity: Option<f64>, summary: &str) -> Self {
        self.client_severity = severity;
        self.client_summary = summary.to_owned();
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SenderSummary {
    pub id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponderSummary {
    pub user_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    pub distance_m: i64,
    pub eta_seconds: i64,
    pub responded_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocationPoint {
    pub latitude: f64,
    pub longitude: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertDetailResponse {
    pub alert_id: String,
    pub sender: SenderSummary,
    pub status: String,
    pub emergency_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub severity: Option<f64>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub summary: String,
    pub initial_location: LocationPoint,
    pub latest_location: LocationPoint,
    #[serde(skip_serializing_if = "is_false")]
    pub masked: bool,
    pub responders_en_route: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub responders: Option<Vec<ResponderSummary>>,
    pub triggered_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancelled_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breadcrumbs: Option<Vec<BreadcrumbResponse>>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct SearchAlertHistoryRequest {
    #[serde(skip)]
    pub user_id: String,
    #[validate(custom(function = "validate_role"))]
    pub role: String,
    #[validate(custom(function = "validate_status_filter"))]
    #[serde(default)]
    pub status: String,
    #[validate(range(min = 1, max = 100000))]
    #[serde(default = "default_page")]
    pub page: i64,
    #[validate(range(min = 1, max = 100))]
    #[serde(default = "default_size")]
    pub size: i64,
    #[serde(default)]
    pub from_ms: Option<i64>,
    #[serde(default)]
    pub to_ms: Option<i64>,
}

fn default_page() -> i64 {
    1
}

fn default_size() -> i64 {
    20
}

impl Default for SearchAlertHistoryRequest {
    fn default() -> Self {
        Self {
            user_id: String::new(),
            role: "sent".into(),
            status: String::new(),
            page: 1,
            size: 20,
            from_ms: None,
            to_ms: None,
        }
    }
}

fn validate_role(value: &str) -> Result<(), validator::ValidationError> {
    match value {
        "sent" | "received" => Ok(()),
        _ => Err(validator::ValidationError::new("role")),
    }
}

fn validate_status_filter(value: &str) -> Result<(), validator::ValidationError> {
    match value {
        "" | "TRIGGERED" | "ACKNOWLEDGED" | "RESOLVED" | "CANCELLED" | "EXPIRED" | "active"
        | "closed" => Ok(()),
        _ => Err(validator::ValidationError::new("status")),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AlertStatusCounts {
    pub total: i64,
    pub active: i64,
    pub resolved: i64,
    pub cancelled: i64,
    pub expired: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AlertRespondedCounts {
    pub total: i64,
    pub resolved: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AlertSummaryResponse {
    pub sent: AlertStatusCounts,
    pub received: AlertStatusCounts,
    pub responded: AlertRespondedCounts,
}

/// Distance to the victim and a walking ETA, using the same helpers as the
/// Go service.
pub fn responder_metrics(
    victim_lat: f64,
    victim_lon: f64,
    responder_lat: f64,
    responder_lon: f64,
) -> (f64, i64) {
    let distance = distance_meters(victim_lat, victim_lon, responder_lat, responder_lon);
    (distance, estimate_eta_seconds(distance) as i64)
}

/// `POST /api/sos/classify` result: audio-derived triage, nothing persisted.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClassifyResponse {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub emergency_type: String,
    #[serde(default)]
    pub severity: f64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub summary: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub transcript: String,
    /// completed | empty
    pub transcription_status: String,
}

/// One stored incident upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertMediaResponse {
    pub id: String,
    pub alert_id: String,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub uploaded_at: i64,
    /// Authenticated download path (`GET`, `Authorization` header).
    pub url: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub transcript: String,
    pub transcription_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcribed_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triage: Option<TriageInfo>,
    #[serde(skip_serializing_if = "is_false")]
    pub escalated: bool,
    #[serde(skip_serializing_if = "is_zero_usize")]
    pub recipients_added: usize,
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}
