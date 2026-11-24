//! Address use case (`address_usecase.go`).
//!
//! Addresses hang off a contact, so every operation first proves the caller
//! owns that contact; otherwise the answer is `404`.

use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;
use validator::Validate;

use crate::domain::Address;
use crate::error::{AppError, AppResult};
use crate::model::{AddressResponse, CreateAddressRequest, UpdateAddressRequest};
use crate::usecase::contact::ContactStore;

#[async_trait]
pub trait AddressStore: Send + Sync {
    async fn create(&self, address: &Address) -> AppResult<()>;
    async fn find(&self, id: &str, contact_id: &str) -> AppResult<Option<Address>>;
    async fn list_by_contact(&self, contact_id: &str) -> AppResult<Vec<Address>>;
    async fn update(&self, address: &Address) -> AppResult<()>;
    async fn delete(&self, id: &str, contact_id: &str) -> AppResult<()>;
}

pub struct AddressUseCase {
    contacts: Arc<dyn ContactStore>,
    addresses: Arc<dyn AddressStore>,
    now_millis: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl AddressUseCase {
    pub fn new(contacts: Arc<dyn ContactStore>, addresses: Arc<dyn AddressStore>) -> Self {
        Self {
            contacts,
            addresses,
            now_millis: Arc::new(crate::usecase::auth::default_now_millis),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.now_millis = clock;
        self
    }

    async fn owned_contact(&self, user_id: &str, contact_id: &str) -> AppResult<()> {
        self.contacts
            .find_by_id_and_user(contact_id, user_id)
            .await?
            .map(|_| ())
            .ok_or_else(|| AppError::NotFound("contact not found".into()))
    }

    pub async fn create(
        &self,
        user_id: &str,
        contact_id: &str,
        request: CreateAddressRequest,
    ) -> AppResult<AddressResponse> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        self.owned_contact(user_id, contact_id).await?;

        let now = (self.now_millis)();
        let address = Address {
            id: Uuid::new_v4().to_string(),
            contact_id: contact_id.to_owned(),
            street: Some(request.street),
            city: Some(request.city),
            province: Some(request.province),
            postal_code: Some(request.postal_code),
            country: Some(request.country),
            created_at: now,
            updated_at: now,
        };

        self.addresses.create(&address).await?;
        Ok(address_response(&address))
    }

    pub async fn list(&self, user_id: &str, contact_id: &str) -> AppResult<Vec<AddressResponse>> {
        self.owned_contact(user_id, contact_id).await?;
        let rows = self.addresses.list_by_contact(contact_id).await?;
        Ok(rows.iter().map(address_response).collect())
    }

    pub async fn get(
        &self,
        user_id: &str,
        contact_id: &str,
        id: &str,
    ) -> AppResult<AddressResponse> {
        self.owned_contact(user_id, contact_id).await?;
        let address = self
            .addresses
            .find(id, contact_id)
            .await?
            .ok_or_else(|| AppError::NotFound("address not found".into()))?;
        Ok(address_response(&address))
    }

    pub async fn update(
        &self,
        user_id: &str,
        contact_id: &str,
        id: &str,
        request: UpdateAddressRequest,
    ) -> AppResult<AddressResponse> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        self.owned_contact(user_id, contact_id).await?;

        let mut address = self
            .addresses
            .find(id, contact_id)
            .await?
            .ok_or_else(|| AppError::NotFound("address not found".into()))?;

        address.street = Some(request.street);
        address.city = Some(request.city);
        address.province = Some(request.province);
        address.postal_code = Some(request.postal_code);
        address.country = Some(request.country);
        address.updated_at = (self.now_millis)();

        self.addresses.update(&address).await?;
        Ok(address_response(&address))
    }

    pub async fn delete(&self, user_id: &str, contact_id: &str, id: &str) -> AppResult<()> {
        self.owned_contact(user_id, contact_id).await?;
        self.addresses
            .find(id, contact_id)
            .await?
            .ok_or_else(|| AppError::NotFound("address not found".into()))?;
        self.addresses.delete(id, contact_id).await?;
        Ok(())
    }
}

pub fn address_response(address: &Address) -> AddressResponse {
    AddressResponse {
        id: address.id.clone(),
        street: address.street.clone().unwrap_or_default(),
        city: address.city.clone().unwrap_or_default(),
        province: address.province.clone().unwrap_or_default(),
        postal_code: address.postal_code.clone().unwrap_or_default(),
        country: address.country.clone().unwrap_or_default(),
        created_at: address.created_at,
        updated_at: address.updated_at,
    }
}
