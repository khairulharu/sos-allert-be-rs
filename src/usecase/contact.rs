//! Contact use case (`contact_usecase.go`).
//!
//! Every read and write is scoped to the session user; a contact that belongs
//! to somebody else answers `404`, never `403` (same as the Go service).

use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;
use validator::Validate;

use crate::domain::Contact;
use crate::error::{AppError, AppResult};
use crate::model::{
    ContactResponse, CreateContactRequest, SearchContactRequest, UpdateContactRequest,
};

#[async_trait]
pub trait ContactStore: Send + Sync {
    async fn create(&self, contact: &Contact) -> AppResult<()>;
    async fn find_by_id_and_user(&self, id: &str, user_id: &str) -> AppResult<Option<Contact>>;
    async fn update(&self, contact: &Contact) -> AppResult<()>;
    async fn delete(&self, id: &str, user_id: &str) -> AppResult<()>;
    async fn search(
        &self,
        user_id: &str,
        filters: &SearchContactRequest,
    ) -> AppResult<(Vec<Contact>, i64)>;
}

/// Kafka hook for contact events; no-op when the producer is disabled.
#[async_trait]
pub trait ContactEvents: Send + Sync {
    async fn contact_changed(&self, contact: &Contact, action: &str);
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopContactEvents;

#[async_trait]
impl ContactEvents for NoopContactEvents {
    async fn contact_changed(&self, _contact: &Contact, _action: &str) {}
}

pub struct ContactUseCase {
    store: Arc<dyn ContactStore>,
    events: Arc<dyn ContactEvents>,
    now_millis: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl ContactUseCase {
    pub fn new(store: Arc<dyn ContactStore>) -> Self {
        Self {
            store,
            events: Arc::new(NoopContactEvents),
            now_millis: Arc::new(crate::usecase::auth::default_now_millis),
        }
    }

    pub fn with_events(mut self, events: Arc<dyn ContactEvents>) -> Self {
        self.events = events;
        self
    }

    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.now_millis = clock;
        self
    }

    pub async fn create(
        &self,
        user_id: &str,
        request: CreateContactRequest,
    ) -> AppResult<ContactResponse> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;

        let now = (self.now_millis)();
        let contact = Contact {
            id: Uuid::new_v4().to_string(),
            first_name: request.first_name,
            last_name: Some(request.last_name),
            email: Some(request.email),
            phone: Some(request.phone),
            user_id: user_id.to_owned(),
            created_at: now,
            updated_at: now,
        };

        self.store.create(&contact).await?;
        self.events.contact_changed(&contact, "created").await;

        Ok(contact_response(&contact))
    }

    pub async fn get(&self, user_id: &str, id: &str) -> AppResult<ContactResponse> {
        let contact = self
            .store
            .find_by_id_and_user(id, user_id)
            .await?
            .ok_or_else(|| AppError::NotFound("contact not found".into()))?;
        Ok(contact_response(&contact))
    }

    pub async fn update(
        &self,
        user_id: &str,
        id: &str,
        request: UpdateContactRequest,
    ) -> AppResult<ContactResponse> {
        let mut contact = self
            .store
            .find_by_id_and_user(id, user_id)
            .await?
            .ok_or_else(|| AppError::NotFound("contact not found".into()))?;

        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;

        contact.first_name = request.first_name;
        contact.last_name = Some(request.last_name);
        contact.email = Some(request.email);
        contact.phone = Some(request.phone);
        contact.updated_at = (self.now_millis)();

        self.store.update(&contact).await?;
        self.events.contact_changed(&contact, "updated").await;

        Ok(contact_response(&contact))
    }

    pub async fn delete(&self, user_id: &str, id: &str) -> AppResult<()> {
        self.store
            .find_by_id_and_user(id, user_id)
            .await?
            .ok_or_else(|| AppError::NotFound("contact not found".into()))?;

        self.store.delete(id, user_id).await?;
        Ok(())
    }

    /// Paged search. `total_page` is `ceil(total / size)`.
    pub async fn search(
        &self,
        user_id: &str,
        request: SearchContactRequest,
    ) -> AppResult<(Vec<ContactResponse>, i64, i64, i64)> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;

        let (contacts, total) = self.store.search(user_id, &request).await?;
        let responses = contacts.iter().map(contact_response).collect();
        let total_page = if request.size > 0 {
            (total + request.size - 1) / request.size
        } else {
            0
        };

        Ok((responses, total, total_page, request.size))
    }
}

pub fn contact_response(contact: &Contact) -> ContactResponse {
    ContactResponse {
        id: contact.id.clone(),
        first_name: contact.first_name.clone(),
        last_name: contact.last_name.clone().unwrap_or_default(),
        email: contact.email.clone().unwrap_or_default(),
        phone: contact.phone.clone().unwrap_or_default(),
        created_at: contact.created_at,
        updated_at: contact.updated_at,
        addresses: None,
    }
}
