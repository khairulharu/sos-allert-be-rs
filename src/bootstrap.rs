//! Dependency wiring, one place per process (the Go `internal/config/app.go`).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::config::Config;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    db: PgPool,
    redis: redis::Client,
}

impl AppState {
    pub fn db(&self) -> &PgPool {
        &self.db
    }

    pub fn redis(&self) -> &redis::Client {
        &self.redis
    }
}

/// Build the pool and Redis client without contacting either service.
///
/// A connection is opened on first use and reported by `/healthz/ready`, so a
/// dependency blip no longer crash-loops the process (the Go service exited at
/// startup instead).
pub fn build_state(config: Config) -> anyhow::Result<AppState> {
    config.validate()?;

    let db = PgPoolOptions::new()
        .max_connections(config.database.pool.max)
        .min_connections(config.database.pool.idle)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Some(Duration::from_secs(
            config.database.pool.lifetime_secs(),
        )))
        .connect_lazy(&config.database.dsn())
        .context("create postgres pool")?;

    let redis = redis::Client::open(config.redis.url()).context("create redis client")?;

    Ok(AppState {
        config: Arc::new(config),
        db,
        redis,
    })
}

/// Apply pending migrations. Called by an explicit subcommand, never on boot:
/// DDL against a live database needs a human.
pub async fn migrate(db: &PgPool) -> anyhow::Result<()> {
    sqlx::migrate!("./migrations")
        .run(db)
        .await
        .context("run migrations")
}
