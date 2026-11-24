//! SOS use case: trigger, lifecycle, breadcrumbs, history, summary.
//!
//! Ported from `trigger_sos_usecase.go`, `sos_lifecycle.go` and
//! `sos_history.go`. Semantics that matter and are preserved:
//!
//! * idempotency replay is checked **before** the per-user limiter, and a
//!   failed trigger releases its claim so the retry is not a `409`;
//! * the fan-out is fail-open after the alert row exists: a recipient lookup
//!   error yields an empty list and the notifier still runs;
//! * with a grace period the trigger dispatches nothing; the due time lives on
//!   the row and the outbox loop fans out later (at-least-once);
//! * a non-participant gets `404`, not `403`, so alert ids are not confirmed;
//! * coordinates are masked to ~100 m for non-participants only.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use uuid::Uuid;
use validator::Validate;

use crate::domain::{Alert, AlertBreadcrumb, AlertResponse, User};
use crate::error::{AppError, AppResult};
use crate::geo::{mask_coordinates, quantize_distance_meters};
use crate::model::sos::{
    responder_metrics, AlertDetailResponse, AlertRespondedCounts, AlertStatusCounts,
    AlertSummaryResponse, BreadcrumbRequest, BreadcrumbResponse, CancelSosRequest,
    CancelSosResponse, LocationPoint, RespondSosRequest, RespondSosResponse, ResponderSummary,
    SearchAlertHistoryRequest, SenderSummary, SosCreatedResponse, TriageInfo, TriggerSosRequest,
};
use crate::usecase::auth::UserStore;

/// Stale after 2 minutes, the same cutoff used for nearby and fan-out.
pub const STALE_THRESHOLD_MS: i64 = 2 * 60 * 1000;
pub const BREADCRUMB_MAX_AGE_MS: i64 = 24 * 60 * 60 * 1000;
pub const BREADCRUMB_FUTURE_SKEW_MS: i64 = 60 * 1000;
pub const MAX_HISTORY_PAGE: i64 = 100_000;
pub const ACTIVE_LIST_LIMIT: i64 = 20;

/// Fields a transition may set. `None` leaves the column untouched.
#[derive(Debug, Clone, Default)]
pub struct AlertUpdate {
    pub status: String,
    pub cancellation_reason: Option<String>,
    pub cancelled_at: Option<i64>,
    pub resolved_at: Option<i64>,
}

#[async_trait]
pub trait AlertStore: Send + Sync {
    async fn create(&self, alert: &Alert) -> AppResult<()>;
    async fn get_by_id(&self, id: &str) -> AppResult<Option<Alert>>;
    async fn list_active_by_sender(&self, sender_id: &str, limit: i64) -> AppResult<Vec<Alert>>;
    async fn list_inbox(&self, user_id: &str, limit: i64) -> AppResult<Vec<Alert>>;
    /// Compare-and-set on the current statuses; `false` means another
    /// transition won and the caller answers `409`.
    async fn transition(&self, id: &str, from: &[&str], to: &AlertUpdate) -> AppResult<bool>;
    async fn count_active(&self) -> AppResult<i64>;
    async fn expire_older_than(&self, before_ms: i64) -> AppResult<u64>;
    async fn purge_breadcrumbs(&self, before_ms: i64) -> AppResult<u64>;
    async fn list_due_dispatch(&self, due_before_ms: i64, limit: i64) -> AppResult<Vec<Alert>>;
    async fn mark_dispatched(&self, alert_id: &str, at_ms: i64) -> AppResult<bool>;
    async fn create_breadcrumb(&self, crumb: &AlertBreadcrumb) -> AppResult<()>;
    async fn list_breadcrumbs(&self, alert_id: &str, limit: i64)
        -> AppResult<Vec<AlertBreadcrumb>>;
    async fn latest_breadcrumb(&self, alert_id: &str) -> AppResult<Option<AlertBreadcrumb>>;
    /// `Conflict` when the responder already answered (unique index).
    async fn create_response(&self, response: &AlertResponse) -> AppResult<()>;
    async fn is_responder(&self, alert_id: &str, user_id: &str) -> AppResult<bool>;
    async fn create_recipients(
        &self,
        alert_id: &str,
        user_ids: &[String],
        notified_at: i64,
    ) -> AppResult<()>;
    async fn list_recipients(&self, alert_id: &str) -> AppResult<Vec<String>>;
    async fn history(&self, request: &SearchAlertHistoryRequest) -> AppResult<(Vec<Alert>, i64)>;
    /// `(sent_by_status, received_by_status, responded_total, responded_resolved)`
    async fn summary_counts(
        &self,
        user_id: &str,
    ) -> AppResult<(HashMap<String, i64>, HashMap<String, i64>, i64, i64)>;
    async fn count_responders_batch(&self, alert_ids: &[String])
        -> AppResult<HashMap<String, i64>>;
    async fn latest_breadcrumbs_batch(
        &self,
        alert_ids: &[String],
    ) -> AppResult<HashMap<String, AlertBreadcrumb>>;
    async fn responded_ids(
        &self,
        user_id: &str,
        alert_ids: &[String],
    ) -> AppResult<HashSet<String>>;
    async fn responder_summaries_batch(
        &self,
        alert_ids: &[String],
    ) -> AppResult<HashMap<String, Vec<ResponderSummary>>>;
    async fn update_triage(
        &self,
        alert_id: &str,
        emergency_type: &str,
        severity: Option<f64>,
        summary: &str,
    ) -> AppResult<()>;
}

/// The location subset the fan-out needs.
#[async_trait]
pub trait SosLocationStore: Send + Sync {
    async fn get(&self, user_id: &str) -> AppResult<Option<crate::domain::LocationCache>>;
    async fn save(&self, longitude: f64, latitude: f64, user_id: &str) -> AppResult<()>;
    /// User ids inside the radius, nearest first.
    async fn search_ids(
        &self,
        longitude: f64,
        latitude: f64,
        radius_meters: f64,
        count: usize,
    ) -> AppResult<Vec<String>>;
    async fn last_seen_batch(&self, user_ids: &[String]) -> AppResult<HashMap<String, i64>>;
}

#[async_trait]
pub trait SosIdempotency: Send + Sync {
    /// `true` when this caller now owns the pending key.
    async fn claim(&self, key: &str) -> AppResult<bool>;
    async fn get(&self, key: &str) -> AppResult<Option<String>>;
    async fn set(&self, key: &str, value: &str) -> AppResult<()>;
    /// Compare-and-delete on the pending marker only.
    async fn release(&self, key: &str) -> AppResult<()>;
}

#[async_trait]
pub trait SosRateLimiter: Send + Sync {
    async fn allow(&self, user_id: &str, limit: u32, window: Duration) -> AppResult<bool>;
}

#[async_trait]
pub trait SosNotifier: Send + Sync {
    async fn notify(&self, recipient_ids: &[String], alert: &Alert) -> AppResult<()>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopNotifier;

#[async_trait]
impl SosNotifier for NoopNotifier {
    async fn notify(&self, _recipient_ids: &[String], _alert: &Alert) -> AppResult<()> {
        Ok(())
    }
}

/// Live-tracking fan-out (filled in by the WebSocket hub).
pub trait TrackHub: Send + Sync {
    fn publish(&self, alert_id: &str, payload: Vec<u8>);
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopHub;

impl TrackHub for NoopHub {
    fn publish(&self, _alert_id: &str, _payload: Vec<u8>) {}
}

#[derive(Debug, Clone)]
pub struct SosSettings {
    pub grace_period: Duration,
    pub dispatch_poll: Duration,
    pub max_recipients: usize,
    pub min_recipients: usize,
    pub radii: Vec<f64>,
    pub rate_limit_per_hour: u32,
    pub rate_limit_window: Duration,
    pub rate_limit_disabled: bool,
    pub expire_after: Duration,
}

impl Default for SosSettings {
    fn default() -> Self {
        Self {
            grace_period: Duration::from_secs(10),
            dispatch_poll: Duration::from_secs(1),
            max_recipients: 50,
            min_recipients: 3,
            radii: vec![1000.0, 2000.0, 5000.0],
            rate_limit_per_hour: 3,
            rate_limit_window: Duration::from_secs(3600),
            rate_limit_disabled: false,
            expire_after: Duration::from_secs(24 * 3600),
        }
    }
}

pub struct SosUseCase {
    alerts: Arc<dyn AlertStore>,
    locations: Option<Arc<dyn SosLocationStore>>,
    idempotency: Option<Arc<dyn SosIdempotency>>,
    rate_limiter: Option<Arc<dyn SosRateLimiter>>,
    notifier: Arc<dyn SosNotifier>,
    users: Option<Arc<dyn UserStore>>,
    hub: Arc<dyn TrackHub>,
    settings: SosSettings,
    now_millis: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl SosUseCase {
    pub fn new(alerts: Arc<dyn AlertStore>, settings: SosSettings) -> Self {
        Self {
            alerts,
            locations: None,
            idempotency: None,
            rate_limiter: None,
            notifier: Arc::new(NoopNotifier),
            users: None,
            hub: Arc::new(NoopHub),
            settings,
            now_millis: Arc::new(crate::usecase::auth::default_now_millis),
        }
    }

    pub fn with_locations(mut self, locations: Arc<dyn SosLocationStore>) -> Self {
        self.locations = Some(locations);
        self
    }

    pub fn with_idempotency(mut self, idempotency: Arc<dyn SosIdempotency>) -> Self {
        self.idempotency = Some(idempotency);
        self
    }

    pub fn with_rate_limiter(mut self, limiter: Arc<dyn SosRateLimiter>) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }

    pub fn with_notifier(mut self, notifier: Arc<dyn SosNotifier>) -> Self {
        self.notifier = notifier;
        self
    }

    pub fn with_users(mut self, users: Arc<dyn UserStore>) -> Self {
        self.users = Some(users);
        self
    }

    pub fn with_hub(mut self, hub: Arc<dyn TrackHub>) -> Self {
        self.hub = hub;
        self
    }

    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.now_millis = clock;
        self
    }

    pub fn settings(&self) -> &SosSettings {
        &self.settings
    }

    // ----------------------------------------------------------------- trigger

    pub async fn trigger(
        &self,
        sender_id: &str,
        request: TriggerSosRequest,
        idempotency_key: Option<&str>,
    ) -> AppResult<SosCreatedResponse> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        if request.latitude.is_some() != request.longitude.is_some() {
            return Err(AppError::BadRequest(
                "latitude and longitude must be sent together".into(),
            ));
        }
        if sender_id.is_empty() {
            return Err(AppError::BadRequest("sender id is required".into()));
        }

        let scoped_key = idempotency_key
            .filter(|key| !key.is_empty())
            .map(|key| format!("{sender_id}:{key}"));

        // Replay before the limiter: retrying a finished trigger is not a new
        // SOS and must not burn quota.
        if let (Some(key), Some(store)) = (&scoped_key, &self.idempotency) {
            if let Some(replay) = self.replay_if_exists(store, sender_id, key).await? {
                return Ok(replay);
            }
        }

        if !self.settings.rate_limit_disabled {
            if let Some(limiter) = &self.rate_limiter {
                match limiter
                    .allow(
                        sender_id,
                        self.settings.rate_limit_per_hour,
                        self.settings.rate_limit_window,
                    )
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        return Err(AppError::TooManyRequests {
                            message: format!(
                                "SOS rate limit exceeded: max {} triggers per hour",
                                self.settings.rate_limit_per_hour
                            ),
                            retry_after_s: self.settings.rate_limit_window.as_secs(),
                        })
                    }
                    // Fail open: an outage must not block a life-safety call.
                    Err(e) => tracing::warn!(error = %e, "rate limiter unavailable, proceeding"),
                }
            }
        }

        let mut claim_held = false;
        if let (Some(key), Some(store)) = (&scoped_key, &self.idempotency) {
            match store.claim(key).await {
                Ok(true) => claim_held = true,
                Ok(false) => {
                    // A concurrent retry may have finished the key meanwhile.
                    if let Some(replay) = self.replay_if_exists(store, sender_id, key).await? {
                        return Ok(replay);
                    }
                    return Err(AppError::Conflict("trigger already in progress".into()));
                }
                Err(e) => tracing::warn!(error = %e, "idempotency claim unavailable, proceeding"),
            }
        }

        let outcome = self
            .record_trigger(sender_id, &request, scoped_key.as_deref(), &mut claim_held)
            .await;

        // A failed trigger must hand its claim back, otherwise its retry is a
        // 409 for the rest of the TTL even though no alert exists.
        if claim_held {
            if let (Some(key), Some(store)) = (&scoped_key, &self.idempotency) {
                if let Err(e) = store.release(key).await {
                    tracing::warn!(error = %e, "could not release idempotency claim");
                }
            }
        }

        outcome
    }

    async fn record_trigger(
        &self,
        sender_id: &str,
        request: &TriggerSosRequest,
        scoped_key: Option<&str>,
        claim_held: &mut bool,
    ) -> AppResult<SosCreatedResponse> {
        let (latitude, longitude) = self.resolve_coordinates(sender_id, request).await?;
        let now = (self.now_millis)();

        let emergency_type = if request.emergency_type.is_empty() {
            "DANGER".to_string()
        } else {
            request.emergency_type.clone()
        };

        let grace_ms = self.settings.grace_period.as_millis() as i64;
        let alert = Alert {
            id: Uuid::new_v4().to_string(),
            sender_id: sender_id.to_owned(),
            longitude,
            latitude,
            triggered_at: now,
            status: crate::domain::alert::status::TRIGGERED.to_string(),
            emergency_type,
            severity: None,
            summary: None,
            accuracy_meters: request.accuracy_meters.map(|v| v as f32),
            battery_percentage: request.battery_percentage,
            cancellation_reason: None,
            resolved_at: None,
            cancelled_at: None,
            dispatch_due_at: if grace_ms > 0 {
                Some(now + grace_ms)
            } else {
                None
            },
            dispatched_at: None,
        };

        self.alerts.create(&alert).await?;

        // Fail-open fan-out: the row exists, so a lookup outage yields an
        // empty recipient list instead of a 500 that notifies nobody.
        let mut degraded = false;
        let (recipients, radius) = match self
            .find_recipients(sender_id, longitude, latitude, 0)
            .await
        {
            Ok(found) => found,
            Err(e) => {
                tracing::error!(error = %e, alert_id = %alert.id, "recipient lookup failed");
                degraded = true;
                (Vec::new(), self.widest_radius())
            }
        };

        if let Err(e) = self
            .alerts
            .create_recipients(&alert.id, &recipients, alert.triggered_at)
            .await
        {
            tracing::warn!(error = %e, "could not record alert recipients");
        }

        if let (Some(key), Some(store)) = (scoped_key, &self.idempotency) {
            // The alert exists: the key now belongs to a finished trigger and
            // must never be released, even if this write fails.
            *claim_held = false;
            let value = format!("{}|{}", alert.id, recipients.len());
            if let Err(e) = store.set(key, &value).await {
                tracing::warn!(error = %e, "could not store idempotency result");
            }
        }

        if grace_ms <= 0 {
            self.send_fanout(&alert, &recipients, degraded).await;
        } else {
            tracing::info!(
                alert_id = %alert.id,
                due_at = alert.dispatch_due_at.unwrap_or_default(),
                "fan-out scheduled after the grace period"
            );
        }

        let _ = radius;
        Ok(SosCreatedResponse {
            alert_id: alert.id,
            status: alert.status,
            grace_period_seconds: self.settings.grace_period.as_secs(),
            recipients_notified: recipients.len(),
            triggered_at: alert.triggered_at,
            triage: None,
            client_severity: request.client_severity,
            client_summary: request.client_summary.clone(),
        })
    }

    /// Publish/notify one fan-out. `degraded` marks a failed lookup: the
    /// notifier still runs so emergency contacts are reached.
    async fn send_fanout(&self, alert: &Alert, recipients: &[String], degraded: bool) {
        if degraded || recipients.is_empty() {
            tracing::warn!(alert_id = %alert.id, "degraded or empty fan-out");
        }
        if let Err(e) = self.notifier.notify(recipients, alert).await {
            tracing::error!(error = %e, alert_id = %alert.id, "notify failed");
        }
    }

    async fn replay_if_exists(
        &self,
        store: &Arc<dyn SosIdempotency>,
        sender_id: &str,
        key: &str,
    ) -> AppResult<Option<SosCreatedResponse>> {
        let value = match store.get(key).await {
            Ok(Some(value)) => value,
            Ok(None) => return Ok(None),
            Err(e) => {
                tracing::warn!(error = %e, "idempotency lookup unavailable, proceeding");
                return Ok(None);
            }
        };

        let (alert_id, count) = match value.split_once('|') {
            Some((id, count)) => (id.to_string(), count.parse::<usize>().unwrap_or(0)),
            None => (value.clone(), 0),
        };

        let Some(alert) = self.alerts.get_by_id(&alert_id).await? else {
            return Ok(None);
        };
        // Defence in depth: only the caller's own alert may replay.
        if !sender_id.is_empty() && alert.sender_id != sender_id {
            tracing::warn!(key, "idempotency key resolved to another sender, ignoring");
            return Ok(None);
        }

        Ok(Some(SosCreatedResponse {
            alert_id: alert.id,
            status: alert.status,
            grace_period_seconds: 0,
            recipients_notified: count,
            triggered_at: alert.triggered_at,
            triage: alert.severity.map(|severity| TriageInfo {
                emergency_type: alert.emergency_type.clone(),
                severity,
                summary: alert.summary.clone().unwrap_or_default(),
            }),
            client_severity: None,
            client_summary: String::new(),
        }))
    }

    async fn resolve_coordinates(
        &self,
        sender_id: &str,
        request: &TriggerSosRequest,
    ) -> AppResult<(f64, f64)> {
        if let (Some(latitude), Some(longitude)) = (request.latitude, request.longitude) {
            if let Some(locations) = &self.locations {
                if let Err(e) = locations.save(longitude, latitude, sender_id).await {
                    tracing::warn!(error = %e, "could not refresh Redis location from SOS body");
                }
            }
            return Ok((latitude, longitude));
        }

        let Some(locations) = &self.locations else {
            return Err(AppError::Unprocessable(
                "location is required: send latitude and longitude in the SOS body or PUT /api/location first".into(),
            ));
        };

        match locations.get(sender_id).await {
            Ok(Some(location)) => Ok((location.latitude, location.longitude)),
            Ok(None) => Err(AppError::Unprocessable(
                "location is required: send latitude and longitude in the SOS body or PUT /api/location first".into(),
            )),
            Err(e) => Err(AppError::Unprocessable(format!(
                "location is required: send latitude and longitude in the SOS body or PUT /api/location first ({e})"
            ))),
        }
    }

    fn widest_radius(&self) -> f64 {
        self.settings.radii.last().copied().unwrap_or(0.0)
    }

    /// Adaptive radius: start at the index chosen by severity and widen until
    /// `min_recipients` fresh users are found, capped at `max_recipients`.
    async fn find_recipients(
        &self,
        sender_id: &str,
        longitude: f64,
        latitude: f64,
        start_index: usize,
    ) -> AppResult<(Vec<String>, f64)> {
        let locations = self
            .locations
            .as_ref()
            .ok_or_else(|| AppError::Unavailable("location store not configured".into()))?;

        let radii = &self.settings.radii;
        if radii.is_empty() {
            return Ok((Vec::new(), 0.0));
        }
        let start = start_index.min(radii.len() - 1);
        let now = (self.now_millis)();
        let max_candidates = (self.settings.max_recipients * 4).max(self.settings.max_recipients);

        let mut recipients: Vec<String> = Vec::new();
        let mut used_radius = self.widest_radius();

        for radius in &radii[start..] {
            used_radius = *radius;
            // +1 so the sender cannot eat a candidate slot.
            let nearby = locations
                .search_ids(longitude, latitude, *radius, max_candidates + 1)
                .await?;

            let mut seen = HashSet::new();
            let mut candidates: Vec<String> = Vec::new();
            for user_id in nearby {
                if user_id == sender_id || !seen.insert(user_id.clone()) {
                    continue;
                }
                candidates.push(user_id);
                if candidates.len() >= max_candidates {
                    break;
                }
            }

            // One HMGET per radius, never one HGET per candidate.
            let last_seen = match locations.last_seen_batch(&candidates).await {
                Ok(map) => map,
                Err(e) => {
                    tracing::warn!(error = %e, "could not read last_seen batch");
                    HashMap::new()
                }
            };

            recipients.clear();
            for user_id in &candidates {
                let Some(last_seen) = last_seen.get(user_id).copied() else {
                    tracing::warn!(user_id, "no last_seen, skipping");
                    continue;
                };
                if now - last_seen > STALE_THRESHOLD_MS {
                    continue;
                }
                recipients.push(user_id.clone());
                if recipients.len() >= self.settings.max_recipients {
                    break;
                }
            }

            if recipients.len() >= self.settings.min_recipients {
                break;
            }
        }

        Ok((recipients, used_radius))
    }

    // --------------------------------------------------------------- lifecycle

    /// `(is_sender, is_responder, can_view)`.
    ///
    /// A notified recipient may view the incident but is not a participant
    /// yet: coordinates stay masked until they actually respond.
    async fn viewer_for(&self, alert: &Alert, user_id: &str) -> AppResult<(bool, bool, bool)> {
        if alert.sender_id == user_id {
            return Ok((true, false, true));
        }
        if self.alerts.is_responder(&alert.id, user_id).await? {
            return Ok((false, true, true));
        }
        let recipients = self.alerts.list_recipients(&alert.id).await?;
        let is_recipient = recipients.iter().any(|id| id == user_id);
        Ok((false, false, is_recipient))
    }

    pub async fn get_alert(
        &self,
        requester: &str,
        alert_id: &str,
    ) -> AppResult<AlertDetailResponse> {
        let alert = self
            .alerts
            .get_by_id(alert_id)
            .await?
            .ok_or_else(|| AppError::NotFound("alert not found".into()))?;

        let (is_sender, is_responder, can_view) = self.viewer_for(&alert, requester).await?;
        // Non-participants get 404, so ids are not confirmed.
        if !can_view {
            return Err(AppError::NotFound("alert not found".into()));
        }
        let is_participant = is_sender || is_responder;

        let details = self
            .build_details(
                std::slice::from_ref(&alert),
                requester,
                DetailOptions {
                    with_responders: true,
                    resolved_viewers: Some(HashMap::from([(alert.id.clone(), is_participant)])),
                },
            )
            .await?;
        Ok(details.into_iter().next().expect("one detail"))
    }

    pub async fn list_active(&self, sender_id: &str) -> AppResult<Vec<AlertDetailResponse>> {
        let alerts = self
            .alerts
            .list_active_by_sender(sender_id, ACTIVE_LIST_LIMIT)
            .await?;
        self.build_details(
            &alerts,
            sender_id,
            DetailOptions {
                with_responders: true,
                resolved_viewers: None,
            },
        )
        .await
    }

    pub async fn list_inbox(&self, user_id: &str) -> AppResult<Vec<AlertDetailResponse>> {
        let alerts = self.alerts.list_inbox(user_id, ACTIVE_LIST_LIMIT).await?;
        self.build_details(
            &alerts,
            user_id,
            DetailOptions {
                with_responders: true,
                resolved_viewers: None,
            },
        )
        .await
    }

    pub async fn cancel(
        &self,
        sender_id: &str,
        alert_id: &str,
        request: CancelSosRequest,
    ) -> AppResult<CancelSosResponse> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;

        let alert = self
            .alerts
            .get_by_id(alert_id)
            .await?
            .ok_or_else(|| AppError::NotFound("alert not found".into()))?;
        if alert.sender_id != sender_id {
            return Err(AppError::Forbidden(
                "only the alert sender can perform this action".into(),
            ));
        }
        self.verify_pin(sender_id, &request.pin).await?;

        if !crate::domain::alert::status::is_active(&alert.status) {
            return Err(AppError::Conflict("invalid alert status transition".into()));
        }

        let reason = if request.reason.is_empty() {
            "FALSE_ALARM".to_string()
        } else {
            request.reason
        };
        let now = (self.now_millis)();
        let changed = self
            .alerts
            .transition(
                alert_id,
                &[
                    crate::domain::alert::status::TRIGGERED,
                    crate::domain::alert::status::ACKNOWLEDGED,
                ],
                &AlertUpdate {
                    status: crate::domain::alert::status::CANCELLED.into(),
                    cancellation_reason: Some(reason),
                    cancelled_at: Some(now),
                    resolved_at: None,
                },
            )
            .await?;
        if !changed {
            return Err(AppError::Conflict("invalid alert status transition".into()));
        }

        self.publish_status(alert_id, crate::domain::alert::status::CANCELLED, now);
        Ok(CancelSosResponse {
            alert_id: alert_id.to_owned(),
            status: crate::domain::alert::status::CANCELLED.into(),
            cancelled_at: now,
        })
    }

    async fn verify_pin(&self, user_id: &str, pin: &str) -> AppResult<()> {
        let Some(users) = &self.users else {
            return Err(AppError::Precondition(
                "safe PIN is not configured for this user".into(),
            ));
        };
        let user: Option<User> = users.find_by_id(user_id).await?;
        match user.and_then(|u| u.safe_pin_hash) {
            None => Err(AppError::Precondition(
                "safe PIN is not configured for this user".into(),
            )),
            Some(hash) if hash.is_empty() => Err(AppError::Precondition(
                "safe PIN is not configured for this user".into(),
            )),
            Some(hash) => {
                if bcrypt::verify(pin, &hash).unwrap_or(false) {
                    Ok(())
                } else {
                    Err(AppError::Forbidden("invalid safe PIN".into()))
                }
            }
        }
    }

    pub async fn acknowledge(
        &self,
        requester: &str,
        alert_id: &str,
    ) -> AppResult<AlertDetailResponse> {
        let alert = self
            .alerts
            .get_by_id(alert_id)
            .await?
            .ok_or_else(|| AppError::NotFound("alert not found".into()))?;
        let (is_sender, is_responder, can_view) = self.viewer_for(&alert, requester).await?;
        if !can_view {
            return Err(AppError::Forbidden(
                "only the sender or a responder can view this alert".into(),
            ));
        }

        if alert.status != crate::domain::alert::status::ACKNOWLEDGED {
            let changed = self
                .alerts
                .transition(
                    alert_id,
                    &[crate::domain::alert::status::TRIGGERED],
                    &AlertUpdate {
                        status: crate::domain::alert::status::ACKNOWLEDGED.into(),
                        ..Default::default()
                    },
                )
                .await?;
            if !changed {
                return Err(AppError::Conflict("invalid alert status transition".into()));
            }
            self.publish_status(
                alert_id,
                crate::domain::alert::status::ACKNOWLEDGED,
                (self.now_millis)(),
            );
        }

        let updated = self
            .alerts
            .get_by_id(alert_id)
            .await?
            .ok_or_else(|| AppError::NotFound("alert not found".into()))?;
        let details = self
            .build_details(
                std::slice::from_ref(&updated),
                requester,
                DetailOptions {
                    with_responders: true,
                    resolved_viewers: Some(HashMap::from([(
                        updated.id.clone(),
                        is_sender || is_responder,
                    )])),
                },
            )
            .await?;
        Ok(details.into_iter().next().expect("one detail"))
    }

    pub async fn resolve(&self, sender_id: &str, alert_id: &str) -> AppResult<AlertDetailResponse> {
        let alert = self
            .alerts
            .get_by_id(alert_id)
            .await?
            .ok_or_else(|| AppError::NotFound("alert not found".into()))?;
        if alert.sender_id != sender_id {
            return Err(AppError::Forbidden(
                "only the alert sender can perform this action".into(),
            ));
        }

        let now = (self.now_millis)();
        let changed = self
            .alerts
            .transition(
                alert_id,
                &[
                    crate::domain::alert::status::TRIGGERED,
                    crate::domain::alert::status::ACKNOWLEDGED,
                ],
                &AlertUpdate {
                    status: crate::domain::alert::status::RESOLVED.into(),
                    resolved_at: Some(now),
                    ..Default::default()
                },
            )
            .await?;
        if !changed {
            return Err(AppError::Conflict("invalid alert status transition".into()));
        }
        self.publish_status(alert_id, crate::domain::alert::status::RESOLVED, now);

        let updated = self
            .alerts
            .get_by_id(alert_id)
            .await?
            .ok_or_else(|| AppError::NotFound("alert not found".into()))?;
        let details = self
            .build_details(
                std::slice::from_ref(&updated),
                sender_id,
                DetailOptions {
                    with_responders: true,
                    resolved_viewers: Some(HashMap::from([(updated.id.clone(), true)])),
                },
            )
            .await?;
        Ok(details.into_iter().next().expect("one detail"))
    }

    pub async fn respond(
        &self,
        responder_id: &str,
        alert_id: &str,
        request: RespondSosRequest,
    ) -> AppResult<RespondSosResponse> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        let (Some(responder_lat), Some(responder_lon)) =
            (request.responder_latitude, request.responder_longitude)
        else {
            return Err(AppError::BadRequest(
                "responder_latitude and responder_longitude are required".into(),
            ));
        };

        let alert = self
            .alerts
            .get_by_id(alert_id)
            .await?
            .ok_or_else(|| AppError::NotFound("alert not found".into()))?;
        let (_, is_responder, can_view) = self.viewer_for(&alert, responder_id).await?;
        if !can_view {
            return Err(AppError::Forbidden(
                "only the sender or a responder can view this alert".into(),
            ));
        }
        if alert.sender_id == responder_id {
            return Err(AppError::Forbidden(
                "the sender cannot respond to their own alert".into(),
            ));
        }
        if !crate::domain::alert::status::is_active(&alert.status) {
            return Err(AppError::Conflict("invalid alert status transition".into()));
        }
        if is_responder {
            return Err(AppError::Conflict(
                "responder has already responded to this alert".into(),
            ));
        }

        // Distance to the latest known position of the victim.
        let (victim_lat, victim_lon) = match self.alerts.latest_breadcrumb(alert_id).await? {
            Some(crumb) => (crumb.latitude, crumb.longitude),
            None => (alert.latitude, alert.longitude),
        };
        let (distance, eta) =
            responder_metrics(victim_lat, victim_lon, responder_lat, responder_lon);
        let now = (self.now_millis)();

        let response = AlertResponse {
            id: Uuid::new_v4().to_string(),
            alert_id: alert_id.to_owned(),
            responder_id: responder_id.to_owned(),
            responder_latitude: responder_lat,
            responder_longitude: responder_lon,
            distance_meters: distance,
            eta_seconds: eta as i32,
            responded_at: now,
        };
        self.alerts.create_response(&response).await?;

        if alert.status == crate::domain::alert::status::TRIGGERED {
            let changed = self
                .alerts
                .transition(
                    alert_id,
                    &[crate::domain::alert::status::TRIGGERED],
                    &AlertUpdate {
                        status: crate::domain::alert::status::ACKNOWLEDGED.into(),
                        ..Default::default()
                    },
                )
                .await?;
            if changed {
                self.publish_status(alert_id, crate::domain::alert::status::ACKNOWLEDGED, now);
            }
        }

        self.publish_responder_added(&response);

        Ok(RespondSosResponse {
            response_id: response.id,
            eta_seconds: eta,
            victim_distance_meters: quantize_distance_meters(distance),
        })
    }

    pub async fn add_breadcrumb(
        &self,
        sender_id: &str,
        alert_id: &str,
        request: BreadcrumbRequest,
    ) -> AppResult<BreadcrumbResponse> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        let (Some(latitude), Some(longitude)) = (request.latitude, request.longitude) else {
            return Err(AppError::BadRequest(
                "latitude and longitude are required".into(),
            ));
        };

        let alert = self
            .alerts
            .get_by_id(alert_id)
            .await?
            .ok_or_else(|| AppError::NotFound("alert not found".into()))?;
        if alert.sender_id != sender_id {
            return Err(AppError::Forbidden(
                "only the alert sender can perform this action".into(),
            ));
        }
        if !crate::domain::alert::status::is_active(&alert.status) {
            return Err(AppError::Conflict("invalid alert status transition".into()));
        }

        let now = (self.now_millis)();
        let recorded_at = match request.timestamp {
            None => now,
            Some(ts) if ts <= 0 || ts < now - BREADCRUMB_MAX_AGE_MS => {
                return Err(AppError::BadRequest(
                    "breadcrumb timestamp is too old".into(),
                ))
            }
            // A future point would sort above every later breadcrumb.
            Some(ts) if ts > now + BREADCRUMB_FUTURE_SKEW_MS => now,
            Some(ts) => ts,
        };

        let crumb = AlertBreadcrumb {
            id: 0,
            alert_id: alert_id.to_owned(),
            latitude,
            longitude,
            speed_mps: request.speed_mps.map(|v| v as f32),
            heading_deg: request.heading_deg.map(|v| v as f32),
            altitude_meters: request.altitude_meters.map(|v| v as f32),
            accuracy_meters: request.accuracy_meters.map(|v| v as f32),
            battery_percentage: request.battery_percentage,
            recorded_at,
        };
        self.alerts.create_breadcrumb(&crumb).await?;

        if let Some(locations) = &self.locations {
            let _ = locations.save(longitude, latitude, sender_id).await;
        }

        let payload = serde_json::json!({
            "type": "LOCATION_UPDATE",
            "alert_id": alert_id,
            "latitude": latitude,
            "longitude": longitude,
            "speed_mps": request.speed_mps,
            "heading_deg": request.heading_deg,
        });
        self.hub.publish(alert_id, payload.to_string().into_bytes());

        Ok(BreadcrumbResponse {
            latitude,
            longitude,
            speed_mps: request.speed_mps,
            heading_deg: request.heading_deg,
            altitude_meters: request.altitude_meters,
            accuracy_meters: request.accuracy_meters,
            battery_percentage: request.battery_percentage,
            recorded_at,
        })
    }

    pub async fn list_breadcrumbs(
        &self,
        requester: &str,
        alert_id: &str,
    ) -> AppResult<Vec<BreadcrumbResponse>> {
        let alert = self
            .alerts
            .get_by_id(alert_id)
            .await?
            .ok_or_else(|| AppError::NotFound("alert not found".into()))?;
        if requester != alert.sender_id && !self.alerts.is_responder(alert_id, requester).await? {
            return Err(AppError::NotFound("alert not found".into()));
        }

        let crumbs = self.alerts.list_breadcrumbs(alert_id, 1000).await?;
        Ok(crumbs
            .iter()
            .map(|c| BreadcrumbResponse {
                latitude: c.latitude,
                longitude: c.longitude,
                speed_mps: c.speed_mps.map(f64::from),
                heading_deg: c.heading_deg.map(f64::from),
                altitude_meters: c.altitude_meters.map(f64::from),
                accuracy_meters: c.accuracy_meters.map(f64::from),
                battery_percentage: c.battery_percentage,
                recorded_at: c.recorded_at,
            })
            .collect())
    }

    pub async fn history(
        &self,
        request: SearchAlertHistoryRequest,
    ) -> AppResult<(Vec<AlertDetailResponse>, i64)> {
        request
            .validate()
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
        if let (Some(from), Some(to)) = (request.from_ms, request.to_ms) {
            if to < from {
                return Err(AppError::BadRequest("to_ms is before from_ms".into()));
            }
        }

        let (alerts, total) = self.alerts.history(&request).await?;
        let details = self
            .build_details(
                &alerts,
                &request.user_id,
                DetailOptions {
                    with_responders: false,
                    resolved_viewers: None,
                },
            )
            .await?;
        Ok((details, total))
    }

    pub async fn summary(&self, user_id: &str) -> AppResult<AlertSummaryResponse> {
        let (sent, received, responded_total, responded_resolved) =
            self.alerts.summary_counts(user_id).await?;
        Ok(AlertSummaryResponse {
            sent: status_counts(&sent),
            received: status_counts(&received),
            responded: AlertRespondedCounts {
                total: responded_total,
                resolved: responded_resolved,
            },
        })
    }

    /// Participant check for the tracking socket.
    pub async fn authorize_tracking(&self, user_id: &str, alert_id: &str) -> AppResult<()> {
        let alert = self
            .alerts
            .get_by_id(alert_id)
            .await?
            .ok_or_else(|| AppError::NotFound("alert not found".into()))?;
        let (_, _, can_view) = self.viewer_for(&alert, user_id).await?;
        if !can_view {
            return Err(AppError::Forbidden(
                "only the sender or a responder can view this alert".into(),
            ));
        }
        Ok(())
    }

    // ------------------------------------------------------------- background

    /// Durable grace-period outbox: dispatch alerts whose due time passed and
    /// that were never dispatched. At-least-once by design.
    pub async fn dispatch_due(&self, limit: i64) -> AppResult<usize> {
        let now = (self.now_millis)();
        let due = self.alerts.list_due_dispatch(now, limit).await?;
        let mut dispatched = 0;

        for alert in due {
            // Re-check the status: a cancelled/resolved alert is skipped and
            // still marked so it never enters the scan again.
            let current = self.alerts.get_by_id(&alert.id).await?;
            let active = current
                .as_ref()
                .map(|a| crate::domain::alert::status::is_active(&a.status))
                .unwrap_or(false);

            if active {
                let recipients = match self
                    .find_recipients(&alert.sender_id, alert.longitude, alert.latitude, 0)
                    .await
                {
                    Ok((found, _)) => found,
                    Err(e) => {
                        tracing::warn!(error = %e, alert_id = %alert.id, "outbox recipient lookup failed");
                        Vec::new()
                    }
                };
                if !recipients.is_empty() {
                    let _ = self
                        .alerts
                        .create_recipients(&alert.id, &recipients, now)
                        .await;
                }
                self.send_fanout(&alert, &recipients, recipients.is_empty())
                    .await;
            } else {
                tracing::info!(alert_id = %alert.id, "alert is closed, skipping dispatch");
            }

            // Latch: `false` means another tick already marked it.
            let _ = self.alerts.mark_dispatched(&alert.id, now).await;
            dispatched += 1;
        }

        Ok(dispatched)
    }

    /// Expire TRIGGERED alerts older than `expire_after` and purge trail
    /// points older than 30 days for terminal alerts. One failing statement
    /// must not stop the other.
    pub async fn housekeeping(&self) -> AppResult<()> {
        let now = (self.now_millis)();
        let expire_before = now - self.settings.expire_after.as_millis() as i64;
        match self.alerts.expire_older_than(expire_before).await {
            Ok(count) => tracing::info!(count, "expired old alerts"),
            Err(e) => tracing::warn!(error = %e, "expire pass failed"),
        }

        let purge_before = now - 30 * 24 * 3600 * 1000;
        match self.alerts.purge_breadcrumbs(purge_before).await {
            Ok(count) => tracing::info!(count, "purged old breadcrumbs"),
            Err(e) => tracing::warn!(error = %e, "breadcrumb purge failed"),
        }
        Ok(())
    }

    pub async fn count_active(&self) -> AppResult<i64> {
        self.alerts.count_active().await
    }

    fn publish_status(&self, alert_id: &str, status: &str, updated_at: i64) {
        let payload = serde_json::json!({
            "type": "ALERT_STATUS",
            "alert_id": alert_id,
            "status": status,
            "updated_at": updated_at,
        });
        self.hub.publish(alert_id, payload.to_string().into_bytes());
    }

    fn publish_responder_added(&self, response: &AlertResponse) {
        let payload = serde_json::json!({
            "type": "RESPONDER_ADDED",
            "alert_id": response.alert_id,
            "responder_id": response.responder_id,
            "distance_m": quantize_distance_meters(response.distance_meters),
            "eta_seconds": response.eta_seconds,
            "responded_at": response.responded_at,
        });
        self.hub
            .publish(&response.alert_id, payload.to_string().into_bytes());
    }

    // ----------------------------------------------------------- detail batch

    async fn build_details(
        &self,
        alerts: &[Alert],
        requester: &str,
        options: DetailOptions,
    ) -> AppResult<Vec<AlertDetailResponse>> {
        if alerts.is_empty() {
            return Ok(Vec::new());
        }

        let ids: Vec<String> = alerts.iter().map(|a| a.id.clone()).collect();
        let mut responder_counts = self.alerts.count_responders_batch(&ids).await?;
        let latest_crumbs = self.alerts.latest_breadcrumbs_batch(&ids).await?;

        // Names of the senders, one query for the page.
        let mut names: HashMap<String, String> = HashMap::new();
        if let Some(users) = &self.users {
            let mut senders: Vec<String> = alerts.iter().map(|a| a.sender_id.clone()).collect();
            senders.sort();
            senders.dedup();
            let names_map = users.names_by_ids(&senders).await.unwrap_or_else(|e| {
                tracing::warn!(error = %e, "could not load alert sender names");
                HashMap::new()
            });
            names = names_map;
        }

        let viewers = match options.resolved_viewers {
            Some(map) => map,
            None => {
                let responded = self.alerts.responded_ids(requester, &ids).await?;
                alerts
                    .iter()
                    .map(|alert| {
                        (
                            alert.id.clone(),
                            alert.sender_id == requester || responded.contains(&alert.id),
                        )
                    })
                    .collect()
            }
        };

        let participant_ids: Vec<String> = alerts
            .iter()
            .filter(|alert| viewers.get(&alert.id).copied().unwrap_or(false))
            .map(|alert| alert.id.clone())
            .collect();

        let rosters = if options.with_responders && !participant_ids.is_empty() {
            self.alerts
                .responder_summaries_batch(&participant_ids)
                .await?
        } else {
            HashMap::new()
        };

        let mut out = Vec::with_capacity(alerts.len());
        for alert in alerts {
            let is_participant = viewers.get(&alert.id).copied().unwrap_or(false);
            let initial = LocationPoint {
                latitude: alert.latitude,
                longitude: alert.longitude,
                updated_at: None,
            };
            let latest = match latest_crumbs.get(&alert.id) {
                Some(crumb) => LocationPoint {
                    latitude: crumb.latitude,
                    longitude: crumb.longitude,
                    updated_at: Some(crumb.recorded_at),
                },
                None => initial.clone(),
            };

            let (initial, latest, masked) = if is_participant {
                (initial, latest, false)
            } else {
                let (lat, lon) = mask_coordinates(initial.latitude, initial.longitude);
                let masked_initial = LocationPoint {
                    latitude: lat,
                    longitude: lon,
                    updated_at: None,
                };
                let (lat, lon) = mask_coordinates(latest.latitude, latest.longitude);
                let masked_latest = LocationPoint {
                    latitude: lat,
                    longitude: lon,
                    updated_at: latest.updated_at,
                };
                (masked_initial, masked_latest, true)
            };

            responder_counts.entry(alert.id.clone()).or_insert(0);
            out.push(AlertDetailResponse {
                alert_id: alert.id.clone(),
                sender: SenderSummary {
                    id: alert.sender_id.clone(),
                    name: names.get(&alert.sender_id).cloned().unwrap_or_default(),
                },
                status: alert.status.clone(),
                emergency_type: alert.emergency_type.clone(),
                severity: alert.severity,
                summary: alert.summary.clone().unwrap_or_default(),
                initial_location: initial,
                latest_location: latest,
                masked,
                responders_en_route: responder_counts[&alert.id],
                responders: if is_participant {
                    rosters.get(&alert.id).cloned()
                } else {
                    None
                },
                triggered_at: alert.triggered_at,
                resolved_at: alert.resolved_at,
                cancelled_at: alert.cancelled_at,
                breadcrumbs: None,
            });
        }

        Ok(out)
    }
}

struct DetailOptions {
    with_responders: bool,
    /// `Some` when the caller already resolved every viewer (single reads).
    resolved_viewers: Option<HashMap<String, bool>>,
}

fn status_counts(counts: &HashMap<String, i64>) -> AlertStatusCounts {
    let get = |status: &str| counts.get(status).copied().unwrap_or(0);
    AlertStatusCounts {
        total: counts.values().sum(),
        active: get(crate::domain::alert::status::TRIGGERED)
            + get(crate::domain::alert::status::ACKNOWLEDGED),
        resolved: get(crate::domain::alert::status::RESOLVED),
        cancelled: get(crate::domain::alert::status::CANCELLED),
        expired: get(crate::domain::alert::status::EXPIRED),
    }
}
