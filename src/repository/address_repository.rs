//! Postgres address store (sqlx).

use async_trait::async_trait;
use sqlx::PgPool;

use crate::domain::Address;
use crate::error::AppResult;
use crate::usecase::address::AddressStore;

const COLUMNS: &str =
    "id, contact_id, street, city, province, postal_code, country, created_at, updated_at";

#[derive(Clone)]
pub struct PgAddressRepository {
    pool: PgPool,
}

impl PgAddressRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AddressStore for PgAddressRepository {
    async fn create(&self, address: &Address) -> AppResult<()> {
        sqlx::query(
            "INSERT INTO addresses (id, contact_id, street, city, province, postal_code, country, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(&address.id)
        .bind(&address.contact_id)
        .bind(&address.street)
        .bind(&address.city)
        .bind(&address.province)
        .bind(&address.postal_code)
        .bind(&address.country)
        .bind(address.created_at)
        .bind(address.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn find(&self, id: &str, contact_id: &str) -> AppResult<Option<Address>> {
        let address = sqlx::query_as::<_, Address>(&format!(
            "SELECT {COLUMNS} FROM addresses WHERE id = $1 AND contact_id = $2"
        ))
        .bind(id)
        .bind(contact_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(address)
    }

    async fn list_by_contact(&self, contact_id: &str) -> AppResult<Vec<Address>> {
        let rows = sqlx::query_as::<_, Address>(&format!(
            "SELECT {COLUMNS} FROM addresses WHERE contact_id = $1 ORDER BY created_at, id"
        ))
        .bind(contact_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn update(&self, address: &Address) -> AppResult<()> {
        sqlx::query(
            "UPDATE addresses SET street = $2, city = $3, province = $4, postal_code = $5, \
                                  country = $6, updated_at = $7 \
             WHERE id = $1 AND contact_id = $8",
        )
        .bind(&address.id)
        .bind(&address.street)
        .bind(&address.city)
        .bind(&address.province)
        .bind(&address.postal_code)
        .bind(&address.country)
        .bind(address.updated_at)
        .bind(&address.contact_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete(&self, id: &str, contact_id: &str) -> AppResult<()> {
        sqlx::query("DELETE FROM addresses WHERE id = $1 AND contact_id = $2")
            .bind(id)
            .bind(contact_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
