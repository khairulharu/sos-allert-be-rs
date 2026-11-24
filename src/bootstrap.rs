//! Dependency wiring, one place per process (the Go `internal/config/app.go`).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::config::Config;
use crate::repository::address_repository::PgAddressRepository;
use crate::repository::alert_repository::PgAlertRepository;
use crate::repository::contact_repository::PgContactRepository;
use crate::repository::emergency_contact_repository::PgEmergencyContactRepository;
use crate::repository::idempotency::RedisIdempotencyRepository;
use crate::repository::location_cache::RedisLocationCacheRepository;
use crate::repository::location_repository::PgLocationRepository;
use crate::repository::rate_limit::RedisRateLimitRepository;
use crate::repository::redis_conn::RedisHandle;
use crate::repository::session::{run_invalidation, CachedSessionStore, RedisSessionStore};
use crate::repository::user_repository::PgUserRepository;
use crate::tracking::Hub;
use crate::usecase::address::AddressUseCase;
use crate::usecase::auth::{AuthUseCase, SessionStore};
use crate::usecase::contact::ContactUseCase;
use crate::usecase::emergency::EmergencyContactUseCase;
use crate::usecase::location::LocationUseCase;
use crate::usecase::sos::{SosSettings, SosUseCase};
use crate::usecase::triage::OfflineClassifier;

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
    sos: Arc<SosUseCase>,
    hub: Arc<Hub>,
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

    pub fn sos(&self) -> &SosUseCase {
        &self.sos
    }

    pub fn hub(&self) -> &Arc<Hub> {
        &self.hub
    }

    pub fn emergency_contacts(&self) -> &EmergencyContactUseCase {
        &self.emergency_contacts
    }

    pub fn rate_limiter(&self) -> &RedisRateLimitRepository {
        &self.rate_limiter
    }

    /// Start the background tasks the web process needs: session invalidation,
    /// the durable grace-period outbox and housekeeping.
    pub fn spawn_background(&self, shutdown: tokio::sync::watch::Receiver<bool>) {
        let cache = self.sessions.clone();
        let redis = self.redis.clone();
        let invalidation_shutdown = shutdown.clone();
        tokio::spawn(async move {
            run_invalidation(redis, cache, invalidation_shutdown).await;
        });

        let sos = self.sos.clone();
        let settings = sos.settings().clone();
        tokio::spawn(async move {
            run_sos_loops(sos, settings, shutdown).await;
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

    let alerts = Arc::new(PgAlertRepository::new(db.clone()));
    let sos_settings = SosSettings {
        grace_period: Duration::from_secs(config.sos.grace_period_seconds),
        dispatch_poll: Duration::from_millis(config.sos.dispatch_poll_interval_ms()),
        max_recipients: config.sos.max_recipients,
        min_recipients: 3,
        radii: vec![1000.0, 2000.0, 5000.0],
        rate_limit_per_hour: config.sos.rate_limit_per_hour,
        rate_limit_window: Duration::from_secs(3600),
        rate_limit_disabled: config.sos.rate_limit_disabled,
        expire_after: Duration::from_secs(24 * 3600),
    };
    let hub = Hub::new(
        config.sos.ws_max_connections,
        config.sos.ws_max_per_ip,
        config.sos.ws_max_per_user,
    );
    let mut sos_case = SosUseCase::new(alerts, sos_settings)
        .with_locations(Arc::new(RedisLocationCacheRepository::new(redis.clone())))
        .with_idempotency(Arc::new(RedisIdempotencyRepository::new(redis.clone())))
        .with_rate_limiter(Arc::new(RedisRateLimitRepository::new(redis.clone())))
        .with_users(Arc::new(PgUserRepository::new(db.clone())))
        .with_hub(hub.clone() as Arc<dyn crate::usecase::sos::TrackHub>);

    // AI triage: only the deterministic offline classifier is ported so far.
    // A configured remote provider still gets offline classification instead
    // of silently losing triage.
    if config.ai.enabled {
        if !config.ai.provider.is_empty() && config.ai.provider != "offline" {
            tracing::warn!(
                provider = %config.ai.provider,
                "remote AI provider not ported yet, using the offline classifier"
            );
        }
        sos_case = sos_case.with_triage(Arc::new(OfflineClassifier));
    }

    let sos = Arc::new(sos_case);

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
        sos,
        hub,
        rate_limiter,
        sessions,
    })
}

/// Dispatch outbox tick plus the housekeeping loop. Both are best effort: a
/// failed tick is logged and the loop keeps running.
async fn run_sos_loops(
    sos: Arc<SosUseCase>,
    settings: SosSettings,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let poll = settings.dispatch_poll.max(Duration::from_millis(100));
    let housekeeping_every = poll.max(Duration::from_secs(60));
    let mut dispatch = tokio::time::interval(poll);
    let mut housekeeping = tokio::time::interval(housekeeping_every);
    // The first housekeeping pass runs immediately, like the Go service.
    dispatch.tick().await;

    loop {
        tokio::select! {
            _ = shutdown.changed() => return,
            _ = dispatch.tick() => {
                match sos.dispatch_due(100).await {
                    Ok(0) => {}
                    Ok(count) => tracing::info!(count, "grace-period fan-out dispatched"),
                    Err(e) => tracing::warn!(error = %e, "dispatch outbox tick failed"),
                }
            }
            _ = housekeeping.tick() => {
                if let Err(e) = sos.housekeeping().await {
                    tracing::warn!(error = %e, "housekeeping failed");
                }
            }
        }
    }
}

/// Apply pending migrations. Called by an explicit subcommand, never on boot:
/// DDL against a live database needs a human.
pub async fn migrate(db: &PgPool) -> anyhow::Result<()> {
    sqlx::migrate!("./migrations")
        .run(db)
        .await
        .context("run migrations")
}
