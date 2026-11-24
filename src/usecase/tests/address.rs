//! Address use case: ownership is proven through the contact store first.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::domain::{Address, Contact};
use crate::error::{AppError, AppResult};
use crate::model::{CreateAddressRequest, SearchContactRequest, UpdateAddressRequest};
use crate::usecase::address::{AddressStore, AddressUseCase};
use crate::usecase::contact::ContactStore;

struct FakeContacts {
    contact: Mutex<Option<Contact>>,
}

#[async_trait]
impl ContactStore for FakeContacts {
    async fn create(&self, _contact: &Contact) -> AppResult<()> {
        Ok(())
    }
    async fn find_by_id_and_user(&self, id: &str, user_id: &str) -> AppResult<Option<Contact>> {
        Ok(self
            .contact
            .lock()
            .unwrap()
            .clone()
            .filter(|c| c.id == id && c.user_id == user_id))
    }
    async fn update(&self, _contact: &Contact) -> AppResult<()> {
        Ok(())
    }
    async fn delete(&self, _id: &str, _user_id: &str) -> AppResult<()> {
        Ok(())
    }
    async fn search(
        &self,
        _user_id: &str,
        _filters: &SearchContactRequest,
    ) -> AppResult<(Vec<Contact>, i64)> {
        Ok((Vec::new(), 0))
    }
}

#[derive(Default)]
struct FakeAddresses {
    rows: Mutex<Vec<Address>>,
}

#[async_trait]
impl AddressStore for FakeAddresses {
    async fn create(&self, address: &Address) -> AppResult<()> {
        self.rows.lock().unwrap().push(address.clone());
        Ok(())
    }
    async fn find(&self, id: &str, contact_id: &str) -> AppResult<Option<Address>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .find(|a| a.id == id && a.contact_id == contact_id)
            .cloned())
    }
    async fn list_by_contact(&self, contact_id: &str) -> AppResult<Vec<Address>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|a| a.contact_id == contact_id)
            .cloned()
            .collect())
    }
    async fn update(&self, address: &Address) -> AppResult<()> {
        let mut rows = self.rows.lock().unwrap();
        if let Some(slot) = rows.iter_mut().find(|a| a.id == address.id) {
            *slot = address.clone();
        }
        Ok(())
    }
    async fn delete(&self, id: &str, contact_id: &str) -> AppResult<()> {
        self.rows
            .lock()
            .unwrap()
            .retain(|a| !(a.id == id && a.contact_id == contact_id));
        Ok(())
    }
}

fn case(owner: &str) -> (AddressUseCase, Arc<FakeAddresses>) {
    let contact = Contact {
        id: "contact-1".into(),
        first_name: "Budi".into(),
        last_name: None,
        email: None,
        phone: None,
        user_id: owner.into(),
        created_at: 1,
        updated_at: 1,
    };
    let contacts = Arc::new(FakeContacts {
        contact: Mutex::new(Some(contact)),
    });
    let addresses = Arc::new(FakeAddresses::default());
    let case = AddressUseCase::new(
        contacts as Arc<dyn ContactStore>,
        addresses.clone() as Arc<dyn AddressStore>,
    )
    .with_clock(Arc::new(|| 1_700_000_000_000));
    (case, addresses)
}

fn address() -> CreateAddressRequest {
    CreateAddressRequest {
        street: "Jl. Merdeka 1".into(),
        city: "Jakarta".into(),
        province: "DKI".into(),
        postal_code: "10110".into(),
        country: "ID".into(),
    }
}

#[tokio::test]
async fn create_list_get_update_delete() {
    let (case, _) = case("user-1");

    let created = case.create("user-1", "contact-1", address()).await.unwrap();
    assert_eq!(created.city, "Jakarta");
    assert_eq!(created.created_at, 1_700_000_000_000);

    let listed = case.list("user-1", "contact-1").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, created.id);

    let fetched = case.get("user-1", "contact-1", &created.id).await.unwrap();
    assert_eq!(fetched.street, "Jl. Merdeka 1");

    let updated = case
        .update(
            "user-1",
            "contact-1",
            &created.id,
            UpdateAddressRequest {
                street: "Jl. Sudirman 2".into(),
                city: "Bandung".into(),
                province: "Jabar".into(),
                postal_code: "40111".into(),
                country: "ID".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.city, "Bandung");

    case.delete("user-1", "contact-1", &created.id)
        .await
        .unwrap();
    assert!(case.list("user-1", "contact-1").await.unwrap().is_empty());
}

#[tokio::test]
async fn a_contact_owned_by_somebody_else_is_not_found() {
    let (case, _) = case("user-1");
    let err = case
        .create("user-2", "contact-1", address())
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::NotFound(_)));

    let err = case.list("user-2", "contact-1").await.unwrap_err();
    assert!(matches!(err, AppError::NotFound(_)));
}

#[tokio::test]
async fn unknown_address_is_not_found() {
    let (case, _) = case("user-1");
    assert!(matches!(
        case.get("user-1", "contact-1", "ghost").await.unwrap_err(),
        AppError::NotFound(_)
    ));
    assert!(matches!(
        case.delete("user-1", "contact-1", "ghost")
            .await
            .unwrap_err(),
        AppError::NotFound(_)
    ));
}

#[tokio::test]
async fn validation_rejects_a_too_long_postal_code() {
    let (case, _) = case("user-1");
    let mut request = address();
    request.postal_code = "12345678901".into();
    assert!(matches!(
        case.create("user-1", "contact-1", request)
            .await
            .unwrap_err(),
        AppError::BadRequest(_)
    ));
}
