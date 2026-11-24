//! Postgres user store (sqlx).

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use crate::domain::User;
use crate::error::{AppError, AppResult};
use crate::usecase::auth::UserStore;

#[derive(Clone)]
pub struct PgUserRepository {
    pool: PgPool,
}

impl PgUserRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UserStore for PgUserRepository {
    async fn count_by_id(&self, id: &str) -> AppResult<i64> {
        let row = sqlx::query("SELECT count(*) AS total FROM users WHERE id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.try_get::<i64, _>("total")?)
    }

    async fn find_by_id(&self, id: &str) -> AppResult<Option<User>> {
        let user = sqlx::query_as::<_, User>(
            "SELECT id, name, password, created_at, updated_at, \
                    safe_pin_hash, fcm_token, apns_token \
             FROM users WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(user)
    }

    async fn create(&self, user: &User) -> AppResult<()> {
        let result = sqlx::query(
            "INSERT INTO users (id, name, password, created_at, updated_at, \
                                safe_pin_hash, fcm_token, apns_token) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(&user.id)
        .bind(&user.name)
        .bind(&user.password)
        .bind(user.created_at)
        .bind(user.updated_at)
        .bind(&user.safe_pin_hash)
        .bind(&user.fcm_token)
        .bind(&user.apns_token)
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(err)) if err.is_unique_violation() => {
                Err(AppError::Conflict("user already exists".into()))
            }
            Err(e) => Err(AppError::Sqlx(e)),
        }
    }

    async fn update(&self, user: &User) -> AppResult<()> {
        sqlx::query(
            "UPDATE users SET name = $2, password = $3, updated_at = $4, \
                              safe_pin_hash = $5, fcm_token = $6, apns_token = $7 \
             WHERE id = $1",
        )
        .bind(&user.id)
        .bind(&user.name)
        .bind(&user.password)
        .bind(user.updated_at)
        .bind(&user.safe_pin_hash)
        .bind(&user.fcm_token)
        .bind(&user.apns_token)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
