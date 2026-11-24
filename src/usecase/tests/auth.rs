//! Auth use case against in-memory ports.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::domain::User;
use crate::error::{AppError, AppResult};
use crate::model::{LoginUserRequest, RegisterUserRequest, UpdateUserRequest};
use crate::usecase::auth::{verify_password, AuthUseCase, SessionStore, UserStore};

#[derive(Default)]
struct FakeUsers {
    rows: Mutex<HashMap<String, User>>,
}

#[async_trait]
impl UserStore for FakeUsers {
    async fn count_by_id(&self, id: &str) -> AppResult<i64> {
        Ok(self.rows.lock().unwrap().contains_key(id) as i64)
    }

    async fn find_by_id(&self, id: &str) -> AppResult<Option<User>> {
        Ok(self.rows.lock().unwrap().get(id).cloned())
    }

    async fn create(&self, user: &User) -> AppResult<()> {
        let mut rows = self.rows.lock().unwrap();
        if rows.contains_key(&user.id) {
            return Err(AppError::Conflict("user already exists".into()));
        }
        rows.insert(user.id.clone(), user.clone());
        Ok(())
    }

    async fn update(&self, user: &User) -> AppResult<()> {
        self.rows
            .lock()
            .unwrap()
            .insert(user.id.clone(), user.clone());
        Ok(())
    }

    async fn names_by_ids(&self, ids: &[String]) -> AppResult<HashMap<String, String>> {
        let rows = self.rows.lock().unwrap();
        Ok(ids
            .iter()
            .filter_map(|id| rows.get(id).map(|user| (id.clone(), user.name.clone())))
            .collect())
    }
}

#[derive(Default)]
struct FakeSessions {
    by_token: Mutex<HashMap<String, String>>,
    by_user: Mutex<HashMap<String, String>>,
    fail: Mutex<bool>,
    last_ttl: Mutex<Option<Duration>>,
}

#[async_trait]
impl SessionStore for FakeSessions {
    async fn create(&self, user_id: &str, token: &str, ttl: Duration) -> AppResult<()> {
        if *self.fail.lock().unwrap() {
            return Err(AppError::Unavailable("down".into()));
        }
        *self.last_ttl.lock().unwrap() = Some(ttl);
        let mut by_user = self.by_user.lock().unwrap();
        if let Some(old) = by_user.get(user_id) {
            self.by_token.lock().unwrap().remove(old);
        }
        by_user.insert(user_id.to_owned(), token.to_owned());
        self.by_token
            .lock()
            .unwrap()
            .insert(token.to_owned(), user_id.to_owned());
        Ok(())
    }

    async fn get(&self, token: &str) -> AppResult<Option<String>> {
        if *self.fail.lock().unwrap() {
            return Err(AppError::Unavailable("down".into()));
        }
        Ok(self.by_token.lock().unwrap().get(token).cloned())
    }

    async fn delete_by_user(&self, user_id: &str) -> AppResult<()> {
        if *self.fail.lock().unwrap() {
            return Err(AppError::Unavailable("down".into()));
        }
        let mut by_user = self.by_user.lock().unwrap();
        if let Some(token) = by_user.remove(user_id) {
            self.by_token.lock().unwrap().remove(&token);
        }
        Ok(())
    }
}

fn usecase() -> (AuthUseCase, Arc<FakeUsers>, Arc<FakeSessions>) {
    let users = Arc::new(FakeUsers::default());
    let sessions = Arc::new(FakeSessions::default());
    let case = AuthUseCase::new(users.clone(), sessions.clone(), Duration::from_secs(3600))
        .with_clock(Arc::new(|| 1_700_000_000_000));
    (case, users, sessions)
}

fn register(id: &str) -> RegisterUserRequest {
    RegisterUserRequest {
        id: id.into(),
        password: "rahasia".into(),
        name: "Budi".into(),
    }
}

#[tokio::test]
async fn register_hashes_the_password_and_omits_the_token() {
    let (case, users, _) = usecase();
    let response = case.create(register("user-1")).await.unwrap();

    assert_eq!(response.id, "user-1");
    assert_eq!(response.name, "Budi");
    assert!(response.token.is_empty());
    assert_eq!(response.created_at, 1_700_000_000_000);

    let stored = users.rows.lock().unwrap().get("user-1").cloned().unwrap();
    assert_ne!(stored.password, "rahasia");
    assert!(verify_password("rahasia", &stored.password));
}

#[tokio::test]
async fn duplicate_registration_is_conflict() {
    let (case, _, _) = usecase();
    case.create(register("user-1")).await.unwrap();
    let err = case.create(register("user-1")).await.unwrap_err();
    assert!(matches!(err, AppError::Conflict(_)));
}

#[tokio::test]
async fn empty_name_is_bad_request() {
    let (case, _, _) = usecase();
    let mut request = register("user-1");
    request.name = String::new();
    let err = case.create(request).await.unwrap_err();
    assert!(matches!(err, AppError::BadRequest(_)));
}

#[tokio::test]
async fn login_returns_a_token_and_stores_the_session() {
    let (case, _, sessions) = usecase();
    case.create(register("user-1")).await.unwrap();

    let response = case
        .login(LoginUserRequest {
            id: "user-1".into(),
            password: "rahasia".into(),
        })
        .await
        .unwrap();

    assert!(!response.token.is_empty());
    assert!(response.id.is_empty(), "login returns the token only");
    assert_eq!(
        *sessions.last_ttl.lock().unwrap(),
        Some(Duration::from_secs(3600))
    );

    let auth = case.verify(&response.token).await.unwrap();
    assert_eq!(auth.id, "user-1");
}

#[tokio::test]
async fn login_twice_invalidates_the_first_token() {
    let (case, _, _) = usecase();
    case.create(register("user-1")).await.unwrap();

    let credentials = || LoginUserRequest {
        id: "user-1".into(),
        password: "rahasia".into(),
    };

    let first = case.login(credentials()).await.unwrap().token;
    let second = case.login(credentials()).await.unwrap().token;

    assert!(matches!(
        case.verify(&first).await,
        Err(AppError::Unauthorized)
    ));
    assert_eq!(case.verify(&second).await.unwrap().id, "user-1");
}

#[tokio::test]
async fn wrong_password_is_unauthorized() {
    let (case, _, _) = usecase();
    case.create(register("user-1")).await.unwrap();

    let err = case
        .login(LoginUserRequest {
            id: "user-1".into(),
            password: "salah".into(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::Unauthorized));
}

#[tokio::test]
async fn unknown_user_is_unauthorized_not_not_found() {
    let (case, _, _) = usecase();
    let err = case
        .login(LoginUserRequest {
            id: "ghost".into(),
            password: "x".into(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::Unauthorized));
}

#[tokio::test]
async fn session_store_failure_on_login_is_unavailable() {
    let (case, _, sessions) = usecase();
    case.create(register("user-1")).await.unwrap();
    *sessions.fail.lock().unwrap() = true;

    let err = case
        .login(LoginUserRequest {
            id: "user-1".into(),
            password: "rahasia".into(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::Unavailable(_)));
}

#[tokio::test]
async fn verify_treats_store_failure_as_unavailable() {
    let (case, _, sessions) = usecase();
    *sessions.fail.lock().unwrap() = true;
    let err = case.verify("tok").await.unwrap_err();
    assert!(matches!(err, AppError::Unavailable(_)));
}

#[tokio::test]
async fn verify_rejects_oversized_and_missing_tokens() {
    let (case, _, _) = usecase();
    assert!(matches!(
        case.verify("").await.unwrap_err(),
        AppError::Unauthorized
    ));
    assert!(matches!(
        case.verify("NOT_FOUND").await.unwrap_err(),
        AppError::Unauthorized
    ));
    let long = "a".repeat(101);
    assert!(matches!(
        case.verify(&long).await.unwrap_err(),
        AppError::Unauthorized
    ));
}

#[tokio::test]
async fn current_returns_the_profile_or_not_found() {
    let (case, _, _) = usecase();
    case.create(register("user-1")).await.unwrap();

    let profile = case.current("user-1").await.unwrap();
    assert_eq!(profile.name, "Budi");

    let err = case.current("ghost").await.unwrap_err();
    assert!(matches!(err, AppError::NotFound(_)));
}

#[tokio::test]
async fn update_sets_name_pin_and_password() {
    let (case, users, _) = usecase();
    case.create(register("user-1")).await.unwrap();

    let response = case
        .update(UpdateUserRequest {
            id: "user-1".into(),
            name: Some("Sari".into()),
            safe_pin: Some("1234".into()),
            password: Some("baru".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(response.name, "Sari");
    let stored = users.rows.lock().unwrap().get("user-1").cloned().unwrap();
    let pin_hash = stored.safe_pin_hash.clone().unwrap();
    assert!(verify_password("1234", &pin_hash));
    assert_ne!(pin_hash, "1234");
    assert!(verify_password("baru", &stored.password));
    assert_eq!(stored.updated_at, 1_700_000_000_000);
}

#[tokio::test]
async fn update_ignores_empty_strings() {
    let (case, users, _) = usecase();
    case.create(register("user-1")).await.unwrap();
    let before = users.rows.lock().unwrap().get("user-1").cloned().unwrap();

    case.update(UpdateUserRequest {
        id: "user-1".into(),
        name: Some(String::new()),
        safe_pin: Some(String::new()),
        ..Default::default()
    })
    .await
    .unwrap();

    let after = users.rows.lock().unwrap().get("user-1").cloned().unwrap();
    assert_eq!(after.name, before.name);
    assert_eq!(after.safe_pin_hash, before.safe_pin_hash);
}

#[tokio::test]
async fn update_rejects_a_non_numeric_pin() {
    let (case, _, _) = usecase();
    case.create(register("user-1")).await.unwrap();
    let err = case
        .update(UpdateUserRequest {
            id: "user-1".into(),
            safe_pin: Some("12a4".into()),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::BadRequest(_)));
}

#[tokio::test]
async fn update_of_a_missing_user_is_not_found() {
    let (case, _, _) = usecase();
    let err = case
        .update(UpdateUserRequest {
            id: "ghost".into(),
            name: Some("x".into()),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::NotFound(_)));
}

#[tokio::test]
async fn logout_clears_the_session_and_is_idempotent() {
    let (case, _, sessions) = usecase();
    case.create(register("user-1")).await.unwrap();
    let token = case
        .login(LoginUserRequest {
            id: "user-1".into(),
            password: "rahasia".into(),
        })
        .await
        .unwrap()
        .token;

    assert!(case.logout("user-1").await.unwrap());
    assert!(matches!(
        case.verify(&token).await,
        Err(AppError::Unauthorized)
    ));
    assert!(case.logout("user-1").await.unwrap());
    assert!(sessions.by_token.lock().unwrap().is_empty());
}

#[tokio::test]
async fn logout_survives_a_missing_user_row() {
    let (case, _, _) = usecase();
    // Never registered: the session delete still must not fail.
    assert!(case.logout("ghost").await.unwrap());
}
