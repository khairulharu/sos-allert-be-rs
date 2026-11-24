//! Postgres push subscriptions.
//!
//! Ownership is decided under a row lock: an endpoint belongs to the browser
//! that registered it first, so a racing or foreign subscribe cannot take it
//! over.

use async_trait::async_trait;
use sqlx::PgPool;

use crate::domain::PushSubscription;
use crate::error::{AppError, AppResult};
use crate::usecase::push::PushSubscriptionStore;

const COLUMNS: &str = "id, user_id, endpoint, p256dh, auth, user_agent, created_at, updated_at";

#[derive(Clone)]
pub struct PgPushSubscriptionRepository {
    pool: PgPool,
}

impl PgPushSubscriptionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PushSubscriptionStore for PgPushSubscriptionRepository {
    async fn upsert(&self, subscription: &PushSubscription) -> AppResult<String> {
        let mut tx = self.pool.begin().await?;

        let existing: Option<(String, String)> = sqlx::query_as(
            "SELECT id, user_id FROM push_subscriptions WHERE endpoint = $1 FOR UPDATE",
        )
        .bind(&subscription.endpoint)
        .fetch_optional(&mut *tx)
        .await?;

        let stored_id = match existing {
            Some((id, user_id)) => {
                if user_id != subscription.user_id {
                    return Err(AppError::Conflict(
                        "push endpoint is already registered to another user".into(),
                    ));
                }
                sqlx::query(
                    "UPDATE push_subscriptions \
                     SET p256dh = $2, auth = $3, user_agent = $4, updated_at = $5 \
                     WHERE id = $1",
                )
                .bind(&id)
                .bind(&subscription.p256dh)
                .bind(&subscription.auth)
                .bind(&subscription.user_agent)
                .bind(subscription.updated_at)
                .execute(&mut *tx)
                .await?;
                id.clone()
            }
            None => {
                sqlx::query(
                    "INSERT INTO push_subscriptions \
                        (id, user_id, endpoint, p256dh, auth, user_agent, created_at, updated_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                )
                .bind(&subscription.id)
                .bind(&subscription.user_id)
                .bind(&subscription.endpoint)
                .bind(&subscription.p256dh)
                .bind(&subscription.auth)
                .bind(&subscription.user_agent)
                .bind(subscription.created_at)
                .bind(subscription.updated_at)
                .execute(&mut *tx)
                .await?;
                subscription.id.clone()
            }
        };

        tx.commit().await?;
        Ok(stored_id)
    }

    async fn list_by_user(&self, user_id: &str) -> AppResult<Vec<PushSubscription>> {
        let rows = sqlx::query_as::<_, PushSubscription>(&format!(
            "SELECT {COLUMNS} FROM push_subscriptions WHERE user_id = $1 ORDER BY created_at ASC"
        ))
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn delete_by_user_and_endpoint(&self, user_id: &str, endpoint: &str) -> AppResult<()> {
        sqlx::query("DELETE FROM push_subscriptions WHERE user_id = $1 AND endpoint = $2")
            .bind(user_id)
            .bind(endpoint)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_by_endpoint(&self, endpoint: &str) -> AppResult<()> {
        sqlx::query("DELETE FROM push_subscriptions WHERE endpoint = $1")
            .bind(endpoint)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
