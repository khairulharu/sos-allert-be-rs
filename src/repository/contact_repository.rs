//! Postgres contact store (sqlx).
//!
//! Filters use `ILIKE` because MySQL's default collation made `LIKE`
//! case-insensitive; Postgres `LIKE` is not, so the Go behaviour needs
//! `ILIKE`. Paging is ordered by `created_at DESC, id` — the Go service left
//! the order to the storage engine, which makes page boundaries unstable.

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use crate::domain::Contact;
use crate::error::AppResult;
use crate::model::SearchContactRequest;
use crate::usecase::contact::ContactStore;

const COLUMNS: &str = "id, first_name, last_name, email, phone, user_id, created_at, updated_at";

#[derive(Clone)]
pub struct PgContactRepository {
    pool: PgPool,
}

impl PgContactRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ContactStore for PgContactRepository {
    async fn create(&self, contact: &Contact) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO contacts (id, first_name, last_name, email, phone, user_id, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(&contact.id)
        .bind(&contact.first_name)
        .bind(&contact.last_name)
        .bind(&contact.email)
        .bind(&contact.phone)
        .bind(&contact.user_id)
        .bind(contact.created_at)
        .bind(contact.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn find_by_id_and_user(&self, id: &str, user_id: &str) -> AppResult<Option<Contact>> {
        let contact = sqlx::query_as::<_, Contact>(&format!(
            "SELECT {COLUMNS} FROM contacts WHERE id = $1 AND user_id = $2"
        ))
        .bind(id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(contact)
    }

    async fn update(&self, contact: &Contact) -> AppResult<()> {
        sqlx::query(
            "UPDATE contacts SET first_name = $2, last_name = $3, email = $4, phone = $5, \
                                 updated_at = $6 \
             WHERE id = $1 AND user_id = $7",
        )
        .bind(&contact.id)
        .bind(&contact.first_name)
        .bind(&contact.last_name)
        .bind(&contact.email)
        .bind(&contact.phone)
        .bind(contact.updated_at)
        .bind(&contact.user_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete(&self, id: &str, user_id: &str) -> AppResult<()> {
        sqlx::query("DELETE FROM contacts WHERE id = $1 AND user_id = $2")
            .bind(id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn search(
        &self,
        user_id: &str,
        filters: &SearchContactRequest,
    ) -> AppResult<(Vec<Contact>, i64)> {
        let name = filters.name.trim();
        let email = filters.email.trim();
        let phone = filters.phone.trim();
        let size = filters.size.clamp(1, 100);
        let offset = (filters.page.max(1) - 1) * size;

        let rows = sqlx::query_as::<_, Contact>(&format!(
            "SELECT {COLUMNS} FROM contacts \
             WHERE user_id = $1 \
               AND ($2 = '' OR first_name ILIKE '%' || $2 || '%' OR last_name ILIKE '%' || $2 || '%') \
               AND ($3 = '' OR email ILIKE '%' || $3 || '%') \
               AND ($4 = '' OR phone ILIKE '%' || $4 || '%') \
             ORDER BY created_at DESC, id \
             LIMIT $5 OFFSET $6"
        ))
        .bind(user_id)
        .bind(name)
        .bind(email)
        .bind(phone)
        .bind(size)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let row = sqlx::query(
            "SELECT count(*) AS total FROM contacts \
             WHERE user_id = $1 \
               AND ($2 = '' OR first_name ILIKE '%' || $2 || '%' OR last_name ILIKE '%' || $2 || '%') \
               AND ($3 = '' OR email ILIKE '%' || $3 || '%') \
               AND ($4 = '' OR phone ILIKE '%' || $4 || '%')",
        )
        .bind(user_id)
        .bind(name)
        .bind(email)
        .bind(phone)
        .fetch_one(&self.pool)
        .await?;
        let total: i64 = row.try_get("total")?;

        Ok((rows, total))
    }
}
