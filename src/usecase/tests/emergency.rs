//! Emergency contact rules: E.164, cap, duplicates.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::domain::EmergencyContact;
use crate::error::{AppError, AppResult};
use crate::usecase::emergency::{
    is_e164, normalize_phone_number, CreateEmergencyContactRequest, EmergencyContactStore,
    EmergencyContactUseCase, MAX_EMERGENCY_CONTACTS_PER_USER,
};

#[derive(Default)]
struct FakeStore {
    rows: Mutex<Vec<EmergencyContact>>,
}

#[async_trait]
impl EmergencyContactStore for FakeStore {
    async fn create(&self, contact: &EmergencyContact) -> AppResult<()> {
        self.rows.lock().unwrap().push(contact.clone());
        Ok(())
    }

    async fn list_by_user(&self, user_id: &str) -> AppResult<Vec<EmergencyContact>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.user_id == user_id)
            .cloned()
            .collect())
    }
}

fn case() -> (EmergencyContactUseCase, Arc<FakeStore>) {
    let store = Arc::new(FakeStore::default());
    let case = EmergencyContactUseCase::new(store.clone() as Arc<dyn EmergencyContactStore>)
        .with_clock(Arc::new(|| 1_700_000_000_000));
    (case, store)
}

fn request(phone: &str) -> CreateEmergencyContactRequest {
    CreateEmergencyContactRequest {
        contact_name: "Ibu".into(),
        phone_number: phone.into(),
        relationship: "family".into(),
    }
}

#[test]
fn e164_rules_match_the_go_regex() {
    assert!(is_e164("+6281234567890"));
    assert!(is_e164("+12345678"), "8 digits is the minimum");
    assert!(!is_e164("+1234567"), "7 digits is too short");
    assert!(!is_e164("+1234567890123456"), "16 digits is too long");
    assert!(!is_e164("+012345678"), "leading zero");
    assert!(!is_e164("6281234567890"), "missing +");
    assert!(!is_e164("+62abc"));
}

#[test]
fn normalization_strips_typed_separators() {
    assert_eq!(
        normalize_phone_number("+62 812-3456-7890"),
        "+6281234567890"
    );
    assert_eq!(
        normalize_phone_number(" +62 (812) 3456.7890 "),
        "+6281234567890"
    );
}

#[tokio::test]
async fn creates_a_contact_with_a_canonical_number() {
    let (case, store) = case();
    let created = case
        .create("user-1", request("+62 812-3456-7890"))
        .await
        .unwrap();

    assert_eq!(created.phone_number, "+6281234567890");
    assert!(!created.is_verified);
    assert_eq!(created.created_at, 1_700_000_000_000);

    let rows = store.rows.lock().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].user_id, "user-1");
}

#[tokio::test]
async fn invalid_numbers_are_unprocessable() {
    let (case, _) = case();
    for phone in ["0812", "+62abc", "1234", "+012345678"] {
        let err = case.create("user-1", request(phone)).await.unwrap_err();
        assert!(
            matches!(err, AppError::Unprocessable(_)),
            "{phone} should be 422, got {err:?}"
        );
    }
}

#[tokio::test]
async fn the_same_number_cannot_be_added_twice() {
    let (case, _) = case();
    case.create("user-1", request("+6281234567890"))
        .await
        .unwrap();

    // Same number, different separators.
    let err = case
        .create("user-1", request("+62 812 3456 7890"))
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::Unprocessable(_)));
}

#[tokio::test]
async fn the_list_is_capped_at_ten() {
    let (case, _) = case();
    for index in 0..MAX_EMERGENCY_CONTACTS_PER_USER {
        case.create("user-1", request(&format!("+62812345678{index:02}")))
            .await
            .unwrap();
    }

    let err = case
        .create("user-1", request("+6289999999999"))
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::Unprocessable(_)));
}

#[tokio::test]
async fn list_is_per_user_and_keeps_legacy_rows() {
    let (case, store) = case();
    case.create("user-1", request("+6281234567890"))
        .await
        .unwrap();

    // A legacy row that predates the E.164 rule stays readable.
    store.rows.lock().unwrap().push(EmergencyContact {
        id: "legacy".into(),
        user_id: "user-1".into(),
        contact_name: "Kakek".into(),
        phone_number: "0812".into(),
        relationship: "family".into(),
        is_verified: true,
        created_at: 1,
    });

    assert_eq!(case.list("user-1").await.unwrap().len(), 2);
    assert!(case.list("user-2").await.unwrap().is_empty());
}

#[tokio::test]
async fn empty_fields_are_bad_requests() {
    let (case, _) = case();
    let err = case
        .create(
            "user-1",
            CreateEmergencyContactRequest {
                contact_name: String::new(),
                phone_number: "+6281234567890".into(),
                relationship: "family".into(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::BadRequest(_)));
}
