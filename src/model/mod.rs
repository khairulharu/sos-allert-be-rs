//! API models: request/response shapes exactly as the Go service emits them.
//!
//! Responses are wrapped in `{"data": ...}` and errors in `{"errors": "..."}`
//! (`model.WebResponse`, `NewErrorHandler`). Fields with `omitempty` in Go use
//! `skip_serializing_if` here.

pub mod contact;
pub mod location;
pub mod sos;

pub use contact::{
    AddressResponse, ContactResponse, CreateAddressRequest, CreateContactRequest,
    SearchContactRequest, UpdateAddressRequest, UpdateContactRequest,
};
pub use location::{
    LocationResponse, LocationStatusResponse, NearbyLocationResponse, NearbyResponder,
    UpdateLocationRequest,
};

use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebResponse<T> {
    pub data: T,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paging: Option<PageMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors: Option<String>,
}

impl<T> WebResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            data,
            paging: None,
            errors: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageMetadata {
    pub page: i64,
    pub size: i64,
    pub total_item: i64,
    pub total_page: i64,
}

/// Error body: the Go error handler renders `{"errors": message}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub errors: String,
}

/// The authenticated caller, resolved from the raw session token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auth {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UserResponse {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub token: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub created_at: i64,
    #[serde(skip_serializing_if = "is_zero")]
    pub updated_at: i64,
}

fn is_zero(value: &i64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RegisterUserRequest {
    #[validate(length(min = 1, max = 100))]
    pub id: String,
    #[validate(length(min = 1, max = 100))]
    pub password: String,
    #[validate(length(min = 1, max = 100))]
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct LoginUserRequest {
    #[validate(length(min = 1, max = 100))]
    pub id: String,
    #[validate(length(min = 1, max = 100))]
    pub password: String,
}

#[derive(Debug, Clone, Deserialize, Default, Validate)]
pub struct UpdateUserRequest {
    /// Set from the session, never from the body.
    #[serde(skip)]
    pub id: String,
    #[validate(length(max = 100))]
    #[serde(default)]
    pub password: Option<String>,
    #[validate(length(max = 100))]
    #[serde(default)]
    pub name: Option<String>,
    /// Four digits, stored as a bcrypt hash. Empty means "leave it alone"
    /// (the Go tag was `omitempty,len=4,numeric`).
    #[validate(custom(function = "validate_optional_pin"))]
    #[serde(default)]
    pub safe_pin: Option<String>,
    #[validate(length(max = 255))]
    #[serde(default)]
    pub fcm_token: Option<String>,
    #[validate(length(max = 255))]
    #[serde(default)]
    pub apns_token: Option<String>,
}

fn validate_optional_pin(value: &str) -> Result<(), validator::ValidationError> {
    if value.is_empty() {
        return Ok(());
    }
    if value.len() == 4 && value.chars().all(|c| c.is_ascii_digit()) {
        Ok(())
    } else {
        Err(validator::ValidationError::new("pin"))
    }
}

/// The optional fields that are actually set, so an empty string means "leave
/// it alone" exactly like the Go `if request.X != ""` checks.
impl UpdateUserRequest {
    pub fn non_empty(value: &Option<String>) -> Option<&str> {
        value.as_deref().filter(|v| !v.is_empty())
    }
}
