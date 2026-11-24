//! Contact use case against an in-memory store.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::domain::Contact;
use crate::error::{AppError, AppResult};
use crate::model::{CreateContactRequest, SearchContactRequest, UpdateContactRequest};
use crate::usecase::contact::{ContactStore, ContactUseCase};

#[derive(Default)]
struct FakeContacts {
    rows: Mutex<HashMap<String, Contact>>,
}

#[async_trait]
impl ContactStore for FakeContacts {
    async fn create(&self, contact: &Contact) -> AppResult<()> {
        self.rows
            .lock()
            .unwrap()
            .insert(contact.id.clone(), contact.clone());
        Ok(())
    }

    async fn find_by_id_and_user(&self, id: &str, user_id: &str) -> AppResult<Option<Contact>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .get(id)
            .filter(|contact| contact.user_id == user_id)
            .cloned())
    }

    async fn update(&self, contact: &Contact) -> AppResult<()> {
        self.rows
            .lock()
            .unwrap()
            .insert(contact.id.clone(), contact.clone());
        Ok(())
    }

    async fn delete(&self, id: &str, user_id: &str) -> AppResult<()> {
        let mut rows = self.rows.lock().unwrap();
        if rows.get(id).map(|c| c.user_id.as_str()) == Some(user_id) {
            rows.remove(id);
        }
        Ok(())
    }

    async fn search(
        &self,
        user_id: &str,
        filters: &SearchContactRequest,
    ) -> AppResult<(Vec<Contact>, i64)> {
        let rows: Vec<Contact> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|contact| contact.user_id == user_id)
            .filter(|contact| {
                filters.name.is_empty()
                    || contact
                        .first_name
                        .to_lowercase()
                        .contains(&filters.name.to_lowercase())
            })
            .cloned()
            .collect();
        let total = rows.len() as i64;
        let offset = ((filters.page.max(1) - 1) * filters.size) as usize;
        let page = rows
            .into_iter()
            .skip(offset)
            .take(filters.size as usize)
            .collect();
        Ok((page, total))
    }
}

fn case() -> (ContactUseCase, Arc<FakeContacts>) {
    let store = Arc::new(FakeContacts::default());
    let case = ContactUseCase::new(store.clone() as Arc<dyn ContactStore>)
        .with_clock(Arc::new(|| 1_700_000_000_000));
    (case, store)
}

fn request(first: &str) -> CreateContactRequest {
    CreateContactRequest {
        first_name: first.into(),
        last_name: "Sitorus".into(),
        email: "a@b.co".into(),
        phone: "0812".into(),
    }
}

#[tokio::test]
async fn create_scopes_the_contact_to_the_caller() {
    let (case, store) = case();
    let contact = case.create("user-1", request("Budi")).await.unwrap();

    assert_eq!(contact.first_name, "Budi");
    assert_eq!(contact.last_name, "Sitorus");
    assert_eq!(contact.created_at, 1_700_000_000_000);
    assert!(contact.addresses.is_none());

    let stored = store.rows.lock().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored.values().next().unwrap().user_id, "user-1");
}

#[tokio::test]
async fn another_users_contact_is_not_found() {
    let (case, _) = case();
    let contact = case.create("user-1", request("Budi")).await.unwrap();

    let err = case.get("user-2", &contact.id).await.unwrap_err();
    assert!(matches!(err, AppError::NotFound(_)));
}

#[tokio::test]
async fn validation_rejects_an_empty_first_name_and_bad_email() {
    let (case, _) = case();

    let mut empty = request("");
    empty.first_name = String::new();
    assert!(matches!(
        case.create("user-1", empty).await.unwrap_err(),
        AppError::BadRequest(_)
    ));

    let mut bad_email = request("Budi");
    bad_email.email = "not-an-email".into();
    assert!(matches!(
        case.create("user-1", bad_email).await.unwrap_err(),
        AppError::BadRequest(_)
    ));
}

#[tokio::test]
async fn update_replaces_every_field_and_bumps_updated_at() {
    let (case, _) = case();
    let created = case.create("user-1", request("Budi")).await.unwrap();

    let updated = case
        .update(
            "user-1",
            &created.id,
            UpdateContactRequest {
                first_name: "Sari".into(),
                last_name: "Wijaya".into(),
                email: "s@w.id".into(),
                phone: "0813".into(),
            },
        )
        .await
        .unwrap();

    assert_eq!(updated.first_name, "Sari");
    assert_eq!(updated.last_name, "Wijaya");
    assert_eq!(updated.email, "s@w.id");
}

#[tokio::test]
async fn update_and_delete_of_a_missing_contact_are_404() {
    let (case, _) = case();
    let update = UpdateContactRequest {
        first_name: "X".into(),
        last_name: String::new(),
        email: String::new(),
        phone: String::new(),
    };

    assert!(matches!(
        case.update("user-1", "ghost", update).await.unwrap_err(),
        AppError::NotFound(_)
    ));
    assert!(matches!(
        case.delete("user-1", "ghost").await.unwrap_err(),
        AppError::NotFound(_)
    ));
}

#[tokio::test]
async fn search_pages_and_reports_total_page() {
    let (case, _) = case();
    for name in ["Budi", "Budi", "Budi", "Sari"] {
        case.create("user-1", request(name)).await.unwrap();
    }
    case.create("user-2", request("Other")).await.unwrap();

    let (rows, total, total_page, size) = case
        .search(
            "user-1",
            SearchContactRequest {
                name: "budi".into(),
                page: 1,
                size: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(total, 3);
    assert_eq!(total_page, 2);
    assert_eq!(size, 2);

    let (rows, total, _, _) = case
        .search(
            "user-1",
            SearchContactRequest {
                name: "budi".into(),
                page: 2,
                size: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(total, 3);
}

#[tokio::test]
async fn search_rejects_a_page_size_above_the_cap() {
    let (case, _) = case();
    let err = case
        .search(
            "user-1",
            SearchContactRequest {
                size: 101,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::BadRequest(_)));
}

#[tokio::test]
async fn delete_removes_the_row() {
    let (case, store) = case();
    let contact = case.create("user-1", request("Budi")).await.unwrap();
    case.delete("user-1", &contact.id).await.unwrap();
    assert!(store.rows.lock().unwrap().is_empty());
}
