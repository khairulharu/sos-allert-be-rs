//! Dependency wiring, one place per process (the Go `internal/config/app.go`).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::config::Config;
use crate::repository::address_repository::PgAddressRepository;
use crate::repository::contact_repository::PgContactRepository;
use crate::repository::emergency_contact_repository::PgEmergencyContactRepository;
use crate::repository::location_cache::RedisLocationCacheRepository;
use crate::repository::location_repository::PgLocationRepository;
use crate::repository::rate_limit::RedisRateLimitRepository;
use crate::repository::redis_conn::RedisHandle;
use crate::repository::session::{run_invalidation, CachedSessionStore, RedisSessionStore};
use crate::repository::user_repository::PgUserRepository;
use crate::usecase::address::AddressUseCase;
use crate::usecase::auth::{AuthUseCase, SessionStore};
use crate::usecase::contact::ContactUseCase;
use crate::usecase::emergency::EmergencyContactUseCase;
use crate::usecase::location::LocationUseCase;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    db: PgPool,
    redis: RedisHandle,
    auth: Arc<AuthUseCase>,
    contacts: Arc<ContactUseCase>,
    addresses: Arc<AddressUseCase>,
    locations: Arc<LocationUseCase>,
    emergency_contacts: Arc<EmergencyContactUseCase>,
    rate_limiter: Arc<RedisRateLimitRepository>,
    sessions: Arc<CachedSessionStore>,
}

impl AppState {
    pub fn db(&self) -> &PgPool {
        &self.db
    }

    pub fn redis_client(&self) -> &redis::Client {
        self.redis.client()
    }

    pub fn redis(&self) -> &RedisHandle {
        &self.redis
    }

    pub fn auth(&self) -> &AuthUseCase {
        &self.auth
    }

    pub fn contacts(&self) -> &ContactUseCase {
        &self.contacts
    }

    pub fn addresses(&self) -> &AddressUseCase {
        &self.addresses
    }

    pub fn locations(&self) -> &LocationUseCase {
        &self.locations
    }

    pub fn emergency_contacts(&self) -> &EmergencyContactUseCase {
        &self.emergency_contacts
    }

    pub fn rate_limiter(&self) -> &RedisRateLimitRepository {
        &self.rate_limiter
    }

    /// Start the background tasks the web process needs (session invalidation
    /// today, the dispatch outbox and housekeeping in phase 3).
    pub fn spawn_background(&self, shutdown: tokio::sync::watch::Receiver<bool>) {
        let cache = self.sessions.clone();
        let redis = self.redis.clone();
        tokio::spawn(async move {
            run_invalidation(redis, cache, shutdown).await;
        });
    }
}

/// Build the pool and Redis client without contacting either service.
///
/// A connection is opened on first use and reported by `/healthz/ready`, so a
/// dependency blip no longer crash-loops the process.
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

    let redis_client = redis::Client::open(config.redis.url()).context("create redis client")?;
    let redis = RedisHandle::new(redis_client);

    let users = Arc::new(PgUserRepository::new(db.clone()));
    let redis_sessions = Arc::new(RedisSessionStore::new(redis.clone()));

    let cache_ttl = Duration::from_secs(config.auth.session_cache_ttl_secs());
    let sessions = Arc::new(
        CachedSessionStore::new(redis_sessions.clone() as Arc<dyn SessionStore>, cache_ttl)
            .with_publisher(redis_sessions),
    );

    let token_ttl =
        Duration::from_secs(config.auth.token_ttl_hours_or_default().max(1) as u64 * 3600);
    let auth = Arc::new(AuthUseCase::new(
        users,
        sessions.clone() as Arc<dyn SessionStore>,
        token_ttl,
    ));

    let contacts = Arc::new(ContactUseCase::new(Arc::new(PgContactRepository::new(
        db.clone(),
    ))));

    let addresses = Arc::new(AddressUseCase::new(
        Arc::new(PgContactRepository::new(db.clone())),
        Arc::new(PgAddressRepository::new(db.clone())),
    ));

    let locations = Arc::new(LocationUseCase::new(
        Arc::new(RedisLocationCacheRepository::new(redis.clone())),
        Some(Arc::new(PgLocationRepository::new(db.clone()))),
    ));

    let emergency_contacts = Arc::new(EmergencyContactUseCase::new(Arc::new(
        PgEmergencyContactRepository::new(db.clone()),
    )));

    let rate_limiter = Arc::new(RedisRateLimitRepository::new(redis.clone()));

    Ok(AppState {
        config: Arc::new(config),
        db,
        redis,
        auth,
        contacts,
        addresses,
        locations,
        emergency_contacts,
        rate_limiter,
        sessions,
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
