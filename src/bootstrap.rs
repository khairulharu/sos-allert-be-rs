//! Dependency wiring, one place per process (the Go `internal/config/app.go`).

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::config::Config;
use crate::gateway::kafka::KafkaPublisher;
use crate::gateway::notify::{
    AlertDispatcher, ApnsNotifier, FcmNotifier, TwilioNotifier, WebPushNotifier,
};
use crate::gateway::publisher::EventPublisher;
use crate::observability::Metrics;
use crate::repository::address_repository::PgAddressRepository;
use crate::repository::alert_repository::PgAlertRepository;
use crate::repository::contact_repository::PgContactRepository;
use crate::repository::emergency_contact_repository::PgEmergencyContactRepository;
use crate::repository::idempotency::RedisIdempotencyRepository;
use crate::repository::location_cache::RedisLocationCacheRepository;
use crate::repository::location_repository::PgLocationRepository;
use crate::repository::push_subscription_repository::PgPushSubscriptionRepository;
use crate::repository::rate_limit::RedisRateLimitRepository;
use crate::repository::redis_conn::RedisHandle;
use crate::repository::session::{run_invalidation, CachedSessionStore, RedisSessionStore};
use crate::repository::user_repository::PgUserRepository;
use crate::storage::LocalStorage;
use crate::tracking::Hub;
use crate::usecase::address::AddressUseCase;
use crate::usecase::auth::{AuthUseCase, SessionStore};
use crate::usecase::classify_store::RedisClassifyStore;
use crate::usecase::contact::ContactUseCase;
use crate::usecase::emergency::EmergencyContactUseCase;
use crate::usecase::location::LocationUseCase;
use crate::usecase::push::PushUseCase;
use crate::usecase::sos::{SosSettings, SosUseCase};
use crate::usecase::stt::WhisperTranscriber;
use crate::usecase::triage::{
    AiTriage, JevClient, OfflineClassifier, OllamaClient, OpenAiClient, Triage,
};

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
    push: Arc<PushUseCase>,
    sos: Arc<SosUseCase>,
    hub: Arc<Hub>,
    metrics: Arc<Metrics>,
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

    pub fn push(&self) -> &PushUseCase {
        &self.push
    }

    pub fn hub(&self) -> &Arc<Hub> {
        &self.hub
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
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

    let push = Arc::new(PushUseCase::new(
        Arc::new(PgPushSubscriptionRepository::new(db.clone())),
        config.webpush.vapid_public_key.clone(),
        config.webpush.enabled,
        config.webpush.allowed_hosts.clone(),
    ));

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
    let metrics = Arc::new(Metrics::new());
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
        .with_hub(hub.clone() as Arc<dyn crate::usecase::sos::TrackHub>)
        .with_metrics(metrics.clone() as Arc<dyn crate::usecase::sos::SosMetrics>)
        .with_storage(Arc::new(LocalStorage::new(
            config.storage.local_dir.clone(),
        )));

    // Kafka fan-out: when the producer is enabled the web process publishes
    // and the worker notifies; otherwise the trigger notifies directly.
    if config.kafka.producer.enabled {
        match KafkaPublisher::new(&config.kafka.bootstrap.servers, Duration::from_secs(10)) {
            Ok(publisher) => {
                tracing::info!("kafka producer enabled: the worker owns the fan-out");
                sos_case = sos_case.with_publisher(Arc::new(publisher) as Arc<dyn EventPublisher>);
            }
            Err(e) => {
                tracing::error!(error = %e, "kafka producer unavailable, notifying directly")
            }
        }
    }

    // Triage chain: the configured live provider (if any), then the offline
    // keyword classifier. Fail-open by construction: a dead provider just
    // means the next stage answers.
    if config.ai.enabled {
        let timeout = config.ai.timeout_seconds.max(1);
        let mut chain: Vec<Arc<dyn Triage>> = Vec::new();
        match config.ai.provider.as_str() {
            "jev" => {
                chain.push(Arc::new(JevClient::new(&config.ai.jev, timeout)));
                if !config.ai.openai.api_key.is_empty() {
                    chain.push(Arc::new(OpenAiClient::new(&config.ai.openai, timeout)));
                }
            }
            "openai" => chain.push(Arc::new(OpenAiClient::new(&config.ai.openai, timeout))),
            "ollama" => chain.push(Arc::new(OllamaClient::new(&config.ai.ollama, timeout))),
            _ => {}
        }
        chain.push(Arc::new(OfflineClassifier));
        tracing::info!(
            provider = %config.ai.provider,
            stages = chain.len(),
            "triage chain configured"
        );
        sos_case = sos_case.with_triage(Arc::new(AiTriage::new(chain)));
    }

    // STT is optional: without it audio is stored untranscribed and
    // /api/sos/classify answers 503.
    if config.ai.stt.enabled {
        tracing::info!(provider = %config.ai.stt.provider, model = %config.ai.stt.model, "STT configured");
        sos_case = sos_case.with_transcriber(Arc::new(WhisperTranscriber::new(&config.ai.stt)));
    }
    sos_case = sos_case.with_classify_store(Arc::new(RedisClassifyStore::new(
        redis.clone(),
        Duration::from_secs(120),
    )));

    // Notification providers. Unconfigured channels stay log-only instead of
    // failing the trigger.
    sos_case = sos_case.with_notifier(Arc::new(build_dispatcher(&config, db.clone())));

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
        push,
        sos,
        hub,
        metrics,
        rate_limiter,
        sessions,
    })
}

/// Push/SMS dispatcher shared by the web process and the worker: both open
/// the same database to resolve device tokens and emergency contacts.
pub fn build_dispatcher(config: &Config, db: PgPool) -> AlertDispatcher {
    let apns_key = if config.apns.enabled {
        match std::fs::read(&config.apns.private_key_path) {
            Ok(pem) => pem,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    path = %config.apns.private_key_path,
                    "cannot read the APNs private key; APNs disabled"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    let subscriptions = Arc::new(PgPushSubscriptionRepository::new(db.clone()));
    let web = config
        .webpush
        .enabled
        .then(|| Arc::new(WebPushNotifier::new(&config.webpush, subscriptions)));

    AlertDispatcher {
        fcm: config.fcm.enabled.then(|| FcmNotifier::new(&config.fcm)),
        apns: Some(ApnsNotifier::new(&config.apns, apns_key)).filter(|n| n.configured()),
        twilio: config
            .twilio
            .enabled
            .then(|| TwilioNotifier::new(&config.twilio)),
        tokens: Some(Arc::new(PgUserRepository::new(db.clone()))),
        contacts: Some(Arc::new(PgEmergencyContactRepository::new(db))),
        web,
    }
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
                if let Err(e) = sos.refresh_active_gauge().await {
                    tracing::warn!(error = %e, "active SOS gauge refresh failed");
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
