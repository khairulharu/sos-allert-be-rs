//! Auth and user use case.
//!
//! Mirrors `internal/usecase/user_usecase.go`:
//! * sessions live in Redis with a TTL; `users.token` no longer exists;
//! * login replaces the previous token (single session);
//! * a session-store error is `503`, never `401`, so clients do not throw a
//!   valid token away during a Redis blip;
//! * wrong credentials and unknown ids are both `401` on login.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;
use validator::Validate;

use crate::domain::User;
use crate::error::{AppError, AppResult};
use crate::model::{Auth, LoginUserRequest, RegisterUserRequest, UpdateUserRequest, UserResponse};

/// bcrypt cost the Go service used (`bcrypt.DefaultCost`).
pub const BCRYPT_COST: u32 = 10;

#[async_trait]
pub trait UserStore: Send + Sync {
    async fn count_by_id(&self, id: &str) -> AppResult<i64>;
    async fn find_by_id(&self, id: &str) -> AppResult<Option<User>>;
    async fn create(&self, user: &User) -> AppResult<()>;
    async fn update(&self, user: &User) -> AppResult<()>;
    /// Display names for a page of alert senders, one query.
    async fn names_by_ids(
        &self,
        ids: &[String],
    ) -> AppResult<std::collections::HashMap<String, String>>;
}

#[async_trait]
pub trait SessionStore: Send + Sync {
    /// Store a new session and invalidate the user's previous token.
    async fn create(&self, user_id: &str, token: &str, ttl: Duration) -> AppResult<()>;
    /// `Ok(None)` for an unknown or expired token.
    async fn get(&self, token: &str) -> AppResult<Option<String>>;
    async fn delete_by_user(&self, user_id: &str) -> AppResult<()>;
}

/// Producer hook for `users`/`user login` events; no-op when Kafka is off.
#[async_trait]
pub trait UserEvents: Send + Sync {
    async fn user_changed(&self, user: &User, action: &str);
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopUserEvents;

#[async_trait]
impl UserEvents for NoopUserEvents {
    async fn user_changed(&self, _user: &User, _action: &str) {}
}

pub struct AuthUseCase {
    users: Arc<dyn UserStore>,
    sessions: Arc<dyn SessionStore>,
    events: Arc<dyn UserEvents>,
    token_ttl: Duration,
    bcrypt_cost: u32,
    now_millis: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl AuthUseCase {
    pub fn new(
        users: Arc<dyn UserStore>,
        sessions: Arc<dyn SessionStore>,
        token_ttl: Duration,
    ) -> Self {
        Self {
            users,
            sessions,
            events: Arc::new(NoopUserEvents),
            token_ttl,
            bcrypt_cost: BCRYPT_COST,
            now_millis: Arc::new(default_now_millis),
        }
    }

    pub fn with_events(mut self, events: Arc<dyn UserEvents>) -> Self {
        self.events = events;
        self
    }

    /// Test hooks: deterministic clock.
    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.now_millis = clock;
        self
    }

    /// Resolve a raw token. `Err(Unauthorized)` for a missing session,
    /// `Err(Unavailable)` when the store itself fails.
    pub async fn verify(&self, token: &str) -> AppResult<Auth> {
        if token.is_empty() || token == "NOT_FOUND" || token.len() > 100 {
            return Err(AppError::Unauthorized);
        }

        match self.sessions.get(token).await {
            Ok(Some(user_id)) => Ok(Auth { id: user_id }),
            Ok(None) => Err(AppError::Unauthorized),
            Err(e) => {
                tracing::warn!(error = %e, "session store unavailable during verify");
                Err(AppError::Unavailable("session store unavailable".into()))
            }
        }
    }

    pub async fn create(&self, request: RegisterUserRequest) -> AppResult<UserResponse> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;

        if self.users.count_by_id(&request.id).await? > 0 {
            return Err(AppError::Conflict("user already exists".into()));
        }

        let now = (self.now_millis)();
        let user = User {
            id: request.id,
            name: request.name,
            password: hash_password(&request.password, self.bcrypt_cost)?,
            created_at: now,
            updated_at: now,
            safe_pin_hash: None,
            fcm_token: None,
            apns_token: None,
        };

        self.users.create(&user).await?;
        self.events.user_changed(&user, "created").await;

        Ok(user_response(&user))
    }

    pub async fn login(&self, request: LoginUserRequest) -> AppResult<UserResponse> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;

        let user = self
            .users
            .find_by_id(&request.id)
            .await?
            .ok_or(AppError::Unauthorized)?;

        if !verify_password(&request.password, &user.password) {
            return Err(AppError::Unauthorized);
        }

        let token = Uuid::new_v4().to_string();
        self.sessions
            .create(&user.id, &token, self.token_ttl)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "session create failed");
                AppError::Unavailable("session store unavailable".into())
            })?;

        // A Kafka failure must not turn a successful login into a 500.
        self.events.user_changed(&user, "login").await;

        Ok(UserResponse {
            token,
            ..Default::default()
        })
    }

    /// Current profile. Runs a single SELECT, no transaction (Go #56).
    pub async fn current(&self, user_id: &str) -> AppResult<UserResponse> {
        if user_id.is_empty() || user_id.len() > 100 {
            return Err(AppError::BadRequest("invalid user id".into()));
        }
        let user = self
            .users
            .find_by_id(user_id)
            .await?
            .ok_or_else(|| AppError::NotFound("user not found".into()))?;
        Ok(user_response(&user))
    }

    pub async fn logout(&self, user_id: &str) -> AppResult<bool> {
        if user_id.is_empty() || user_id.len() > 100 {
            return Err(AppError::BadRequest("invalid user id".into()));
        }

        // Session first: logout must succeed even if the user row is gone.
        self.sessions.delete_by_user(user_id).await.map_err(|e| {
            tracing::warn!(error = %e, "session delete failed");
            AppError::Unavailable("session store unavailable".into())
        })?;

        if let Ok(Some(user)) = self.users.find_by_id(user_id).await {
            self.events.user_changed(&user, "logout").await;
        }

        Ok(true)
    }

    pub async fn update(&self, request: UpdateUserRequest) -> AppResult<UserResponse> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;

        let mut user = self
            .users
            .find_by_id(&request.id)
            .await?
            .ok_or_else(|| AppError::NotFound("user not found".into()))?;

        if let Some(name) = UpdateUserRequest::non_empty(&request.name) {
            user.name = name.to_owned();
        }
        if let Some(password) = UpdateUserRequest::non_empty(&request.password) {
            user.password = hash_password(password, self.bcrypt_cost)?;
        }
        if let Some(pin) = UpdateUserRequest::non_empty(&request.safe_pin) {
            user.safe_pin_hash = Some(hash_password(pin, self.bcrypt_cost)?);
        }
        if let Some(token) = UpdateUserRequest::non_empty(&request.fcm_token) {
            user.fcm_token = Some(token.to_owned());
        }
        if let Some(token) = UpdateUserRequest::non_empty(&request.apns_token) {
            user.apns_token = Some(token.to_owned());
        }

        user.updated_at = (self.now_millis)();
        self.users.update(&user).await?;
        self.events.user_changed(&user, "updated").await;

        Ok(user_response(&user))
    }
}

fn user_response(user: &User) -> UserResponse {
    UserResponse {
        id: user.id.clone(),
        name: user.name.clone(),
        token: String::new(),
        created_at: user.created_at,
        updated_at: user.updated_at,
    }
}

pub fn hash_password(raw: &str, cost: u32) -> AppResult<String> {
    bcrypt::hash(raw, cost).map_err(|e| AppError::Internal(anyhow::anyhow!("bcrypt hash: {e}")))
}

pub fn verify_password(raw: &str, hash: &str) -> bool {
    bcrypt::verify(raw, hash).unwrap_or(false)
}

pub fn default_now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
