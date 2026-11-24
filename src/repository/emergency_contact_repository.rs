//! Postgres emergency contact store.

use async_trait::async_trait;
use sqlx::PgPool;

use crate::domain::EmergencyContact;
use crate::error::AppResult;
use crate::usecase::emergency::EmergencyContactStore;

const COLUMNS: &str =
    "id, user_id, contact_name, phone_number, relationship, is_verified, created_at";

#[derive(Clone)]
pub struct PgEmergencyContactRepository {
    pool: PgPool,
}

impl PgEmergencyContactRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EmergencyContactStore for PgEmergencyContactRepository {
    async fn create(&self, contact: &EmergencyContact) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO emergency_contacts \
                (id, user_id, contact_name, phone_number, relationship, is_verified, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&contact.id)
        .bind(&contact.user_id)
        .bind(&contact.contact_name)
        .bind(&contact.phone_number)
        .bind(&contact.relationship)
        .bind(contact.is_verified)
        .bind(contact.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_by_user(&self, user_id: &str) -> AppResult<Vec<EmergencyContact>> {
        let rows = sqlx::query_as::<_, EmergencyContact>(&format!(
            "SELECT {COLUMNS} FROM emergency_contacts WHERE user_id = $1 ORDER BY created_at ASC"
        ))
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
}
