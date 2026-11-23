//! `alerts` and its satellites: breadcrumbs, responses, recipients, media,
//! emergency contacts.

use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Lifecycle states stored in `alerts.status` (varchar, not an enum type).
pub mod status {
    pub const TRIGGERED: &str = "TRIGGERED";
    pub const ACKNOWLEDGED: &str = "ACKNOWLEDGED";
    pub const RESOLVED: &str = "RESOLVED";
    pub const CANCELLED: &str = "CANCELLED";
    pub const EXPIRED: &str = "EXPIRED";

    /// Statuses that still count as "open" for `GET /api/sos/active`.
    pub const ACTIVE: [&str; 2] = [TRIGGERED, ACKNOWLEDGED];

    pub fn is_active(value: &str) -> bool {
        matches!(value, TRIGGERED | ACKNOWLEDGED)
    }

    pub fn is_closed(value: &str) -> bool {
        matches!(value, RESOLVED | CANCELLED | EXPIRED)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Alert {
    pub id: String,
    pub sender_id: String,
    pub longitude: f64,
    pub latitude: f64,
    pub triggered_at: i64,
    pub status: String,
    pub emergency_type: String,
    /// `None` means never classified (NULL), distinct from a real score of 0.
    pub severity: Option<f64>,
    pub summary: Option<String>,
    pub accuracy_meters: Option<f32>,
    pub battery_percentage: Option<i32>,
    pub cancellation_reason: Option<String>,
    pub resolved_at: Option<i64>,
    pub cancelled_at: Option<i64>,
    /// `None` when the alert skipped the grace-period outbox.
    pub dispatch_due_at: Option<i64>,
    pub dispatched_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AlertBreadcrumb {
    pub id: i64,
    pub alert_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub speed_mps: Option<f32>,
    pub heading_deg: Option<f32>,
    pub altitude_meters: Option<f32>,
    pub accuracy_meters: Option<f32>,
    pub battery_percentage: Option<i32>,
    pub recorded_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AlertResponse {
    pub id: String,
    pub alert_id: String,
    pub responder_id: String,
    pub responder_latitude: f64,
    pub responder_longitude: f64,
    pub distance_meters: f64,
    pub eta_seconds: i32,
    pub responded_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AlertRecipient {
    pub alert_id: String,
    pub user_id: String,
    pub notified_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct AlertMedia {
    pub id: String,
    pub alert_id: String,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: i64,
    pub storage_path: String,
    pub uploaded_by: String,
    pub uploaded_at: i64,
    pub transcript: Option<String>,
    /// `disabled` | `pending` | `done` | `empty` | `failed`
    pub transcription_status: String,
    pub transcribed_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EmergencyContact {
    pub id: String,
    pub user_id: String,
    pub contact_name: String,
    pub phone_number: String,
    pub relationship: String,
    pub is_verified: bool,
    pub created_at: i64,
}
