//! Contact and address API models.

use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AddressResponse {
    pub id: String,
    pub street: String,
    pub city: String,
    pub province: String,
    pub postal_code: String,
    pub country: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContactResponse {
    pub id: String,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub phone: String,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub addresses: Option<Vec<AddressResponse>>,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct CreateContactRequest {
    #[validate(length(min = 1, max = 100))]
    pub first_name: String,
    #[serde(default)]
    #[validate(length(max = 100))]
    pub last_name: String,
    #[serde(default)]
    #[validate(length(max = 200), email)]
    pub email: String,
    #[serde(default)]
    #[validate(length(max = 20))]
    pub phone: String,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct UpdateContactRequest {
    #[validate(length(min = 1, max = 100))]
    pub first_name: String,
    #[serde(default)]
    #[validate(length(max = 100))]
    pub last_name: String,
    #[serde(default)]
    #[validate(length(max = 200), email)]
    pub email: String,
    #[serde(default)]
    #[validate(length(max = 20))]
    pub phone: String,
}

/// Query string for `GET /api/contacts`. `Page` is 1-based, `size` defaults
/// to 10 and is capped at 100 by validation.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(default)]
pub struct SearchContactRequest {
    #[validate(length(max = 100))]
    pub name: String,
    #[validate(length(max = 200))]
    pub email: String,
    #[validate(length(max = 20))]
    pub phone: String,
    #[validate(range(min = 1))]
    pub page: i64,
    #[validate(range(min = 1, max = 100))]
    pub size: i64,
}

impl Default for SearchContactRequest {
    fn default() -> Self {
        Self {
            name: String::new(),
            email: String::new(),
            phone: String::new(),
            page: 1,
            size: 10,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct CreateAddressRequest {
    #[serde(default)]
    #[validate(length(max = 255))]
    pub street: String,
    #[serde(default)]
    #[validate(length(max = 255))]
    pub city: String,
    #[serde(default)]
    #[validate(length(max = 255))]
    pub province: String,
    #[serde(default)]
    #[validate(length(max = 10))]
    pub postal_code: String,
    #[serde(default)]
    #[validate(length(max = 100))]
    pub country: String,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct UpdateAddressRequest {
    #[serde(default)]
    #[validate(length(max = 255))]
    pub street: String,
    #[serde(default)]
    #[validate(length(max = 255))]
    pub city: String,
    #[serde(default)]
    #[validate(length(max = 255))]
    pub province: String,
    #[serde(default)]
    #[validate(length(max = 10))]
    pub postal_code: String,
    #[serde(default)]
    #[validate(length(max = 100))]
    pub country: String,
}
