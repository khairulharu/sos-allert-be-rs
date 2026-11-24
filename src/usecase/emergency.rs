//! Tier-1 emergency contacts (`emergency_contacts`).
//!
//! Every contact is an SMS/voice target on each alert, so the list is capped
//! at 10 per user, numbers must be E.164, and a number can only be stored once
//! per user. Violations are `422` via the SOS error mapping in the Go service.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;

use crate::domain::EmergencyContact;
use crate::error::{AppError, AppResult};

/// A Tier-1 list is an SMS amplifier if unbounded.
pub const MAX_EMERGENCY_CONTACTS_PER_USER: usize = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmergencyContactResponse {
    pub id: String,
    pub contact_name: String,
    pub phone_number: String,
    pub relationship: String,
    pub is_verified: bool,
    pub created_at: i64,
}

#[derive(Debug, Clone, Deserialize, Validate)]
pub struct CreateEmergencyContactRequest {
    #[validate(length(min = 1, max = 100))]
    pub contact_name: String,
    #[validate(length(min = 1, max = 30))]
    pub phone_number: String,
    #[validate(length(min = 1, max = 50))]
    pub relationship: String,
}

#[async_trait]
pub trait EmergencyContactStore: Send + Sync {
    async fn create(&self, contact: &EmergencyContact) -> AppResult<()>;
    async fn list_by_user(&self, user_id: &str) -> AppResult<Vec<EmergencyContact>>;
}

pub struct EmergencyContactUseCase {
    store: Arc<dyn EmergencyContactStore>,
    now_millis: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl EmergencyContactUseCase {
    pub fn new(store: Arc<dyn EmergencyContactStore>) -> Self {
        Self {
            store,
            now_millis: Arc::new(crate::usecase::auth::default_now_millis),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.now_millis = clock;
        self
    }

    pub async fn create(
        &self,
        user_id: &str,
        request: CreateEmergencyContactRequest,
    ) -> AppResult<EmergencyContactResponse> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;

        let phone = normalize_phone_number(&request.phone_number);
        if !is_e164(&phone) {
            return Err(AppError::Unprocessable("invalid phone number".into()));
        }

        let existing = self.store.list_by_user(user_id).await?;
        if existing.len() >= MAX_EMERGENCY_CONTACTS_PER_USER {
            return Err(AppError::Unprocessable(
                "too many emergency contacts".into(),
            ));
        }
        // Legacy rows are compared normalized too, so "+62 812..." still
        // blocks a duplicate of the same number.
        if existing
            .iter()
            .any(|c| normalize_phone_number(&c.phone_number) == phone)
        {
            return Err(AppError::Unprocessable(
                "emergency contact already exists".into(),
            ));
        }

        let contact = EmergencyContact {
            id: Uuid::new_v4().to_string(),
            user_id: user_id.to_owned(),
            contact_name: request.contact_name,
            phone_number: phone,
            relationship: request.relationship,
            is_verified: false,
            created_at: (self.now_millis)(),
        };

        self.store.create(&contact).await?;
        Ok(contact_response(&contact))
    }

    /// Rows stored before the E.164 rule stay readable: the list never
    /// filters or rewrites them.
    pub async fn list(&self, user_id: &str) -> AppResult<Vec<EmergencyContactResponse>> {
        let rows = self.store.list_by_user(user_id).await?;
        Ok(rows.iter().map(contact_response).collect())
    }
}

fn contact_response(contact: &EmergencyContact) -> EmergencyContactResponse {
    EmergencyContactResponse {
        id: contact.id.clone(),
        contact_name: contact.contact_name.clone(),
        phone_number: contact.phone_number.clone(),
        relationship: contact.relationship.clone(),
        is_verified: contact.is_verified,
        created_at: contact.created_at,
    }
}

/// Strip the separators people type: spaces, dashes, parentheses, dots.
pub fn normalize_phone_number(raw: &str) -> String {
    raw.trim()
        .chars()
        .filter(|c| !matches!(c, ' ' | '-' | '(' | ')' | '.'))
        .collect()
}

/// E.164: `+`, then 8-15 digits with a non-zero first digit.
pub fn is_e164(phone: &str) -> bool {
    let digits = match phone.strip_prefix('+') {
        Some(rest) => rest,
        None => return false,
    };
    if digits.len() < 8 || digits.len() > 15 {
        return false;
    }
    if digits.starts_with('0') {
        return false;
    }
    digits.chars().all(|c| c.is_ascii_digit())
}
