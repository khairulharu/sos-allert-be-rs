//! Shared Redis connection holder.
//!
//! One multiplexed connection per process, created on first use so a missing
//! Redis does not prevent boot. A connection error drops the cached handle, so
//! the next call reconnects instead of pinning a dead socket.

use std::sync::Arc;

use redis::aio::MultiplexedConnection;
use tokio::sync::OnceCell;

use crate::error::{AppError, AppResult};

#[derive(Clone)]
pub struct RedisHandle {
    client: redis::Client,
    conn: Arc<OnceCell<MultiplexedConnection>>,
}

impl RedisHandle {
    pub fn new(client: redis::Client) -> Self {
        Self {
            client,
            conn: Arc::new(OnceCell::new()),
        }
    }

    pub fn client(&self) -> &redis::Client {
        &self.client
    }

    pub async fn conn(&self) -> AppResult<MultiplexedConnection> {
        if let Some(conn) = self.conn.get() {
            return Ok(conn.clone());
        }
        let conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| AppError::Unavailable(format!("redis connect: {e}")))?;
        // Another task may have won the race; either handle works.
        let _ = self.conn.set(conn.clone());
        Ok(conn)
    }
}
