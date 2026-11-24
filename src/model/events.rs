//! Kafka event payloads (`internal/model/*_event.go`).

use serde::{Deserialize, Serialize};

/// One record on `sos.dispatched`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SosEvent {
    pub event_type: String,
    pub alert_id: String,
    pub sender_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub emergency_type: String,
    pub status: String,
    pub recipients: Vec<String>,
    pub triggered_at: i64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub severity: f64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_severity: Option<f64>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub client_summary: String,
    /// Post-trigger escalation: only the added responders, no second SMS.
    #[serde(default, skip_serializing_if = "is_false")]
    pub push_only: bool,
}

impl SosEvent {
    /// What the workers' notifier needs, including the sender's claim.
    pub fn to_notice(&self) -> crate::model::sos::AlertNotice {
        crate::model::sos::AlertNotice {
            alert_id: self.alert_id.clone(),
            sender_id: self.sender_id.clone(),
            status: self.status.clone(),
            emergency_type: self.emergency_type.clone(),
            severity: (self.severity != 0.0).then_some(self.severity),
            summary: self.summary.clone(),
            client_severity: self.client_severity,
            client_summary: self.client_summary.clone(),
            latitude: self.latitude,
            longitude: self.longitude,
            triggered_at: self.triggered_at,
        }
    }
}

fn is_zero_f64(value: &f64) -> bool {
    *value == 0.0
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UserEvent {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContactEvent {
    pub id: String,
    pub first_name: String,
    #[serde(default)]
    pub last_name: String,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub phone: String,
    pub user_id: String,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AddressEvent {
    pub id: String,
    pub contact_id: String,
    #[serde(default)]
    pub street: String,
    #[serde(default)]
    pub city: String,
    #[serde(default)]
    pub province: String,
    #[serde(default)]
    pub postal_code: String,
    #[serde(default)]
    pub country: String,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub updated_at: i64,
}

/// Topic names used by the producers and the worker.
pub mod topics {
    pub const USERS: &str = "users";
    pub const CONTACTS: &str = "contacts";
    pub const ADDRESSES: &str = "addresses";
    pub const SOS_DISPATCHED: &str = "sos.dispatched";
    pub const SOS_DLQ: &str = "sos.dispatched.dlq";
}
