//! SOS use case against in-memory ports: trigger semantics, idempotency,
//! rate limiting, adaptive radius, outbox, lifecycle, masking and history.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;

use crate::domain::{Alert, AlertBreadcrumb, AlertMedia, AlertResponse, LocationCache, User};
use crate::error::{AppError, AppResult};
use crate::model::sos::{
    BreadcrumbRequest, CancelSosRequest, RespondSosRequest, SearchAlertHistoryRequest,
    TriggerSosRequest,
};
use crate::model::sos::{ResponderSummary, TriageInfo};
use crate::usecase::auth::UserStore;
use crate::usecase::sos::{
    AlertStore, AlertUpdate, SosIdempotency, SosLocationStore, SosNotifier, SosRateLimiter,
    SosSettings, SosUseCase, TrackHub,
};
use crate::usecase::triage::OfflineClassifier;

const NOW: i64 = 1_700_000_000_000;

#[derive(Default)]
struct FakeAlerts {
    rows: Mutex<HashMap<String, Alert>>,
    recipients: Mutex<HashMap<String, Vec<String>>>,
    breadcrumbs: Mutex<Vec<AlertBreadcrumb>>,
    responses: Mutex<Vec<AlertResponse>>,
    media: Mutex<Vec<AlertMedia>>,
}

impl FakeAlerts {
    fn active(&self) -> Vec<Alert> {
        self.rows
            .lock()
            .unwrap()
            .values()
            .filter(|alert| crate::domain::alert::status::is_active(&alert.status))
            .cloned()
            .collect()
    }
}

#[async_trait]
impl AlertStore for FakeAlerts {
    async fn create(&self, alert: &Alert) -> AppResult<()> {
        self.rows
            .lock()
            .unwrap()
            .insert(alert.id.clone(), alert.clone());
        Ok(())
    }

    async fn get_by_id(&self, id: &str) -> AppResult<Option<Alert>> {
        Ok(self.rows.lock().unwrap().get(id).cloned())
    }

    async fn list_active_by_sender(&self, sender_id: &str, limit: i64) -> AppResult<Vec<Alert>> {
        let mut rows = self.active();
        rows.retain(|alert| alert.sender_id == sender_id);
        rows.sort_by_key(|alert| -alert.triggered_at);
        rows.truncate(limit as usize);
        Ok(rows)
    }

    async fn list_inbox(&self, user_id: &str, limit: i64) -> AppResult<Vec<Alert>> {
        let ids: HashSet<String> = self
            .recipients
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, users)| users.iter().any(|u| u == user_id))
            .map(|(alert_id, _)| alert_id.clone())
            .collect();
        let mut rows: Vec<Alert> = self
            .active()
            .into_iter()
            .filter(|alert| ids.contains(&alert.id))
            .collect();
        rows.sort_by_key(|alert| -alert.triggered_at);
        rows.truncate(limit as usize);
        Ok(rows)
    }

    async fn transition(&self, id: &str, from: &[&str], to: &AlertUpdate) -> AppResult<bool> {
        let mut rows = self.rows.lock().unwrap();
        let Some(alert) = rows.get_mut(id) else {
            return Ok(false);
        };
        if !from.contains(&alert.status.as_str()) {
            return Ok(false);
        }
        alert.status = to.status.clone();
        if let Some(reason) = &to.cancellation_reason {
            alert.cancellation_reason = Some(reason.clone());
        }
        if to.cancelled_at.is_some() {
            alert.cancelled_at = to.cancelled_at;
        }
        if to.resolved_at.is_some() {
            alert.resolved_at = to.resolved_at;
        }
        Ok(true)
    }

    async fn count_active(&self) -> AppResult<i64> {
        Ok(self.active().len() as i64)
    }

    async fn expire_older_than(&self, before_ms: i64) -> AppResult<u64> {
        let mut rows = self.rows.lock().unwrap();
        let mut count = 0;
        for alert in rows.values_mut() {
            if alert.status == crate::domain::alert::status::TRIGGERED
                && alert.triggered_at < before_ms
            {
                alert.status = crate::domain::alert::status::EXPIRED.into();
                count += 1;
            }
        }
        Ok(count)
    }

    async fn purge_breadcrumbs(&self, before_ms: i64) -> AppResult<u64> {
        let mut crumbs = self.breadcrumbs.lock().unwrap();
        let before = crumbs.len();
        crumbs.retain(|crumb| crumb.recorded_at >= before_ms);
        Ok((before - crumbs.len()) as u64)
    }

    async fn list_due_dispatch(&self, due_before_ms: i64, limit: i64) -> AppResult<Vec<Alert>> {
        let mut rows: Vec<Alert> = self
            .rows
            .lock()
            .unwrap()
            .values()
            .filter(|alert| {
                alert.status == crate::domain::alert::status::TRIGGERED
                    && alert
                        .dispatch_due_at
                        .is_some_and(|due| due <= due_before_ms)
                    && alert.dispatched_at.is_none()
            })
            .cloned()
            .collect();
        rows.sort_by_key(|alert| alert.dispatch_due_at);
        rows.truncate(limit as usize);
        Ok(rows)
    }

    async fn mark_dispatched(&self, alert_id: &str, at_ms: i64) -> AppResult<bool> {
        let mut rows = self.rows.lock().unwrap();
        let Some(alert) = rows.get_mut(alert_id) else {
            return Ok(false);
        };
        if alert.dispatched_at.is_some() {
            return Ok(false);
        }
        alert.dispatched_at = Some(at_ms);
        Ok(true)
    }

    async fn create_breadcrumb(&self, crumb: &AlertBreadcrumb) -> AppResult<()> {
        self.breadcrumbs.lock().unwrap().push(crumb.clone());
        Ok(())
    }

    async fn list_breadcrumbs(
        &self,
        alert_id: &str,
        limit: i64,
    ) -> AppResult<Vec<AlertBreadcrumb>> {
        let mut rows: Vec<AlertBreadcrumb> = self
            .breadcrumbs
            .lock()
            .unwrap()
            .iter()
            .filter(|crumb| crumb.alert_id == alert_id)
            .cloned()
            .collect();
        rows.sort_by_key(|crumb| crumb.recorded_at);
        rows.truncate(limit as usize);
        Ok(rows)
    }

    async fn latest_breadcrumb(&self, alert_id: &str) -> AppResult<Option<AlertBreadcrumb>> {
        Ok(self
            .breadcrumbs
            .lock()
            .unwrap()
            .iter()
            .filter(|crumb| crumb.alert_id == alert_id)
            .max_by_key(|crumb| crumb.recorded_at)
            .cloned())
    }

    async fn create_response(&self, response: &AlertResponse) -> AppResult<()> {
        let mut responses = self.responses.lock().unwrap();
        if responses
            .iter()
            .any(|r| r.alert_id == response.alert_id && r.responder_id == response.responder_id)
        {
            return Err(AppError::Conflict(
                "responder has already responded to this alert".into(),
            ));
        }
        responses.push(response.clone());
        Ok(())
    }

    async fn is_responder(&self, alert_id: &str, user_id: &str) -> AppResult<bool> {
        Ok(self
            .responses
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.alert_id == alert_id && r.responder_id == user_id))
    }

    async fn create_recipients(
        &self,
        alert_id: &str,
        user_ids: &[String],
        _notified_at: i64,
    ) -> AppResult<()> {
        let mut map = self.recipients.lock().unwrap();
        let entry = map.entry(alert_id.to_owned()).or_default();
        for id in user_ids {
            if !entry.contains(id) {
                entry.push(id.clone());
            }
        }
        Ok(())
    }

    async fn list_recipients(&self, alert_id: &str) -> AppResult<Vec<String>> {
        Ok(self
            .recipients
            .lock()
            .unwrap()
            .get(alert_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn history(&self, request: &SearchAlertHistoryRequest) -> AppResult<(Vec<Alert>, i64)> {
        let rows = self.rows.lock().unwrap();
        let mut items: Vec<Alert> = rows
            .values()
            .filter(|alert| {
                let status_ok = match request.status.as_str() {
                    "" => true,
                    "active" => crate::domain::alert::status::is_active(&alert.status),
                    "closed" => !crate::domain::alert::status::is_active(&alert.status),
                    other => alert.status == other,
                };
                if !status_ok {
                    return false;
                }
                if request.role == "received" {
                    self.recipients
                        .lock()
                        .unwrap()
                        .get(&alert.id)
                        .map(|users| users.contains(&request.user_id))
                        .unwrap_or(false)
                } else {
                    alert.sender_id == request.user_id
                }
            })
            .cloned()
            .collect();
        items.sort_by_key(|alert| -alert.triggered_at);
        let total = items.len() as i64;
        Ok((items, total))
    }

    async fn summary_counts(
        &self,
        user_id: &str,
    ) -> AppResult<(HashMap<String, i64>, HashMap<String, i64>, i64, i64)> {
        let rows = self.rows.lock().unwrap();
        let mut sent = HashMap::new();
        let mut received = HashMap::new();
        for alert in rows.values() {
            if alert.sender_id == user_id {
                *sent.entry(alert.status.clone()).or_insert(0) += 1;
            }
            if self
                .recipients
                .lock()
                .unwrap()
                .get(&alert.id)
                .map(|users| users.iter().any(|u| u == user_id))
                .unwrap_or(false)
            {
                *received.entry(alert.status.clone()).or_insert(0) += 1;
            }
        }
        let responses = self.responses.lock().unwrap();
        let total = responses
            .iter()
            .filter(|r| r.responder_id == user_id)
            .map(|r| r.alert_id.clone())
            .collect::<HashSet<_>>()
            .len() as i64;
        Ok((sent, received, total, 0))
    }

    async fn count_responders_batch(
        &self,
        alert_ids: &[String],
    ) -> AppResult<HashMap<String, i64>> {
        let responses = self.responses.lock().unwrap();
        let mut out = HashMap::new();
        for id in alert_ids {
            let count = responses
                .iter()
                .filter(|r| &r.alert_id == id)
                .map(|r| r.responder_id.clone())
                .collect::<HashSet<_>>()
                .len() as i64;
            out.insert(id.clone(), count);
        }
        Ok(out)
    }

    async fn latest_breadcrumbs_batch(
        &self,
        alert_ids: &[String],
    ) -> AppResult<HashMap<String, AlertBreadcrumb>> {
        let crumbs = self.breadcrumbs.lock().unwrap();
        let mut out = HashMap::new();
        for id in alert_ids {
            if let Some(latest) = crumbs
                .iter()
                .filter(|crumb| &crumb.alert_id == id)
                .max_by_key(|crumb| crumb.recorded_at)
            {
                out.insert(id.clone(), latest.clone());
            }
        }
        Ok(out)
    }

    async fn responded_ids(
        &self,
        user_id: &str,
        alert_ids: &[String],
    ) -> AppResult<HashSet<String>> {
        Ok(self
            .responses
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.responder_id == user_id && alert_ids.contains(&r.alert_id))
            .map(|r| r.alert_id.clone())
            .collect())
    }

    async fn responder_summaries_batch(
        &self,
        alert_ids: &[String],
    ) -> AppResult<HashMap<String, Vec<ResponderSummary>>> {
        let responses = self.responses.lock().unwrap();
        let mut out: HashMap<String, Vec<ResponderSummary>> = HashMap::new();
        for response in responses.iter().filter(|r| alert_ids.contains(&r.alert_id)) {
            out.entry(response.alert_id.clone())
                .or_default()
                .push(ResponderSummary {
                    user_id: response.responder_id.clone(),
                    name: String::new(),
                    distance_m: crate::geo::quantize_distance_meters(response.distance_meters),
                    eta_seconds: response.eta_seconds as i64,
                    responded_at: response.responded_at,
                });
        }
        Ok(out)
    }

    async fn create_media(&self, media: &AlertMedia) -> AppResult<()> {
        self.media.lock().unwrap().push(media.clone());
        Ok(())
    }

    async fn list_media(&self, alert_id: &str) -> AppResult<Vec<AlertMedia>> {
        Ok(self
            .media
            .lock()
            .unwrap()
            .iter()
            .filter(|media| media.alert_id == alert_id)
            .cloned()
            .collect())
    }

    async fn get_media(&self, alert_id: &str, media_id: &str) -> AppResult<Option<AlertMedia>> {
        Ok(self
            .media
            .lock()
            .unwrap()
            .iter()
            .find(|media| media.alert_id == alert_id && media.id == media_id)
            .cloned())
    }

    async fn set_media_transcription(
        &self,
        media_id: &str,
        transcript: Option<&str>,
        status: &str,
        transcribed_at: Option<i64>,
    ) -> AppResult<()> {
        let mut media = self.media.lock().unwrap();
        if let Some(row) = media.iter_mut().find(|row| row.id == media_id) {
            if let Some(text) = transcript {
                row.transcript = Some(text.to_owned());
            }
            row.transcription_status = status.to_owned();
            row.transcribed_at = transcribed_at;
        }
        Ok(())
    }

    async fn update_triage(
        &self,
        alert_id: &str,
        emergency_type: &str,
        severity: Option<f64>,
        summary: &str,
    ) -> AppResult<()> {
        let mut rows = self.rows.lock().unwrap();
        if let Some(alert) = rows.get_mut(alert_id) {
            if !emergency_type.is_empty() {
                alert.emergency_type = emergency_type.to_owned();
            }
            if severity.is_some() {
                alert.severity = severity;
            }
            if !summary.is_empty() {
                alert.summary = Some(summary.to_owned());
            }
        }
        Ok(())
    }
}

struct FakeLocations {
    positions: Mutex<HashMap<String, LocationCache>>,
    /// Users returned by `search_ids` per radius, keyed by radius.
    rings: Mutex<HashMap<u64, Vec<String>>>,
    last_seen: Mutex<HashMap<String, i64>>,
    fail_search: Mutex<bool>,
    saves: AtomicUsize,
}

impl Default for FakeLocations {
    fn default() -> Self {
        Self {
            positions: Mutex::new(HashMap::new()),
            rings: Mutex::new(HashMap::new()),
            last_seen: Mutex::new(HashMap::new()),
            fail_search: Mutex::new(false),
            saves: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl SosLocationStore for FakeLocations {
    async fn get(&self, user_id: &str) -> AppResult<Option<LocationCache>> {
        Ok(self.positions.lock().unwrap().get(user_id).cloned())
    }

    async fn save(&self, longitude: f64, latitude: f64, user_id: &str) -> AppResult<()> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        self.positions.lock().unwrap().insert(
            user_id.to_owned(),
            LocationCache::new(user_id, longitude, latitude),
        );
        self.last_seen
            .lock()
            .unwrap()
            .insert(user_id.to_owned(), NOW);
        Ok(())
    }

    async fn search_ids(
        &self,
        _longitude: f64,
        _latitude: f64,
        radius_meters: f64,
        _count: usize,
    ) -> AppResult<Vec<String>> {
        if *self.fail_search.lock().unwrap() {
            return Err(AppError::Unavailable("redis down".into()));
        }
        Ok(self
            .rings
            .lock()
            .unwrap()
            .get(&(radius_meters as u64))
            .cloned()
            .unwrap_or_default())
    }

    async fn last_seen_batch(&self, user_ids: &[String]) -> AppResult<HashMap<String, i64>> {
        let seen = self.last_seen.lock().unwrap();
        Ok(user_ids
            .iter()
            .filter_map(|id| seen.get(id).map(|value| (id.clone(), *value)))
            .collect())
    }
}

#[derive(Default)]
struct FakeIdempotency {
    pending: Mutex<HashSet<String>>,
    finished: Mutex<HashMap<String, String>>,
    releases: AtomicUsize,
}

#[async_trait]
impl SosIdempotency for FakeIdempotency {
    async fn claim(&self, key: &str) -> AppResult<bool> {
        if self.finished.lock().unwrap().contains_key(key) {
            return Ok(false);
        }
        Ok(self.pending.lock().unwrap().insert(key.to_owned()))
    }

    async fn get(&self, key: &str) -> AppResult<Option<String>> {
        Ok(self.finished.lock().unwrap().get(key).cloned())
    }

    async fn set(&self, key: &str, value: &str) -> AppResult<()> {
        self.pending.lock().unwrap().remove(key);
        self.finished
            .lock()
            .unwrap()
            .insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    async fn release(&self, key: &str) -> AppResult<()> {
        self.releases.fetch_add(1, Ordering::SeqCst);
        self.pending.lock().unwrap().remove(key);
        Ok(())
    }
}

#[derive(Default)]
struct FakeLimiter {
    calls: AtomicUsize,
    allow: Mutex<bool>,
    fail: Mutex<bool>,
}

#[async_trait]
impl SosRateLimiter for FakeLimiter {
    async fn allow(&self, _user_id: &str, _limit: u32, _window: Duration) -> AppResult<bool> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if *self.fail.lock().unwrap() {
            return Err(AppError::Unavailable("down".into()));
        }
        Ok(*self.allow.lock().unwrap())
    }
}

#[derive(Default)]
struct FakeNotifier {
    calls: Mutex<Vec<(Vec<String>, String)>>,
}

#[async_trait]
impl SosNotifier for FakeNotifier {
    async fn notify(&self, recipient_ids: &[String], alert: &Alert) -> AppResult<()> {
        self.calls
            .lock()
            .unwrap()
            .push((recipient_ids.to_vec(), alert.id.clone()));
        Ok(())
    }
}

#[derive(Default)]
struct FakeHub {
    published: Mutex<Vec<(String, String)>>,
}

impl TrackHub for FakeHub {
    fn publish(&self, alert_id: &str, payload: Vec<u8>) {
        self.published.lock().unwrap().push((
            alert_id.to_owned(),
            String::from_utf8_lossy(&payload).into_owned(),
        ));
    }
}

struct FakeUsers {
    pin_hash: Mutex<Option<String>>,
}

#[async_trait]
impl UserStore for FakeUsers {
    async fn count_by_id(&self, _id: &str) -> AppResult<i64> {
        Ok(1)
    }
    async fn find_by_id(&self, id: &str) -> AppResult<Option<User>> {
        Ok(Some(User {
            id: id.into(),
            name: "Sari".into(),
            password: String::new(),
            created_at: 1,
            updated_at: 1,
            safe_pin_hash: self.pin_hash.lock().unwrap().clone(),
            fcm_token: None,
            apns_token: None,
        }))
    }
    async fn create(&self, _user: &User) -> AppResult<()> {
        Ok(())
    }
    async fn update(&self, _user: &User) -> AppResult<()> {
        Ok(())
    }
    async fn names_by_ids(&self, ids: &[String]) -> AppResult<HashMap<String, String>> {
        Ok(ids
            .iter()
            .map(|id| (id.clone(), "Sari".to_owned()))
            .collect())
    }
}

struct Harness {
    case: SosUseCase,
    alerts: Arc<FakeAlerts>,
    locations: Arc<FakeLocations>,
    idempotency: Arc<FakeIdempotency>,
    limiter: Arc<FakeLimiter>,
    notifier: Arc<FakeNotifier>,
    hub: Arc<FakeHub>,
    users: Arc<FakeUsers>,
}

impl Harness {
    fn new(settings: SosSettings) -> Self {
        let alerts = Arc::new(FakeAlerts::default());
        let locations = Arc::new(FakeLocations::default());
        let idempotency = Arc::new(FakeIdempotency::default());
        let limiter = Arc::new(FakeLimiter::default());
        let notifier = Arc::new(FakeNotifier::default());
        let hub = Arc::new(FakeHub::default());
        let users = Arc::new(FakeUsers {
            pin_hash: Mutex::new(Some(bcrypt::hash("1234", 4).expect("hash pin"))),
        });

        let mut limiter_default = false;
        *limiter.allow.lock().unwrap() = limiter_default;
        // Default: allowed.
        limiter_default = true;
        *limiter.allow.lock().unwrap() = limiter_default;

        let case = SosUseCase::new(alerts.clone() as Arc<dyn AlertStore>, settings)
            .with_locations(locations.clone() as Arc<dyn SosLocationStore>)
            .with_idempotency(idempotency.clone() as Arc<dyn SosIdempotency>)
            .with_rate_limiter(limiter.clone() as Arc<dyn SosRateLimiter>)
            .with_notifier(notifier.clone() as Arc<dyn SosNotifier>)
            .with_hub(hub.clone() as Arc<dyn TrackHub>)
            .with_users(users.clone() as Arc<dyn UserStore>)
            .with_clock(Arc::new(|| NOW));

        Self {
            case,
            alerts,
            locations,
            idempotency,
            limiter,
            notifier,
            hub,
            users,
        }
    }

    fn with_triage(mut self) -> Self {
        self.case = self.case.with_triage(Arc::new(OfflineClassifier));
        self
    }

    fn zero_grace() -> SosSettings {
        SosSettings {
            grace_period: Duration::ZERO,
            ..SosSettings::default()
        }
    }
}

fn with_coords(latitude: f64, longitude: f64) -> TriggerSosRequest {
    TriggerSosRequest {
        latitude: Some(latitude),
        longitude: Some(longitude),
        ..Default::default()
    }
}

/// Three fresh responders inside 1 km, one stale user that must be skipped.
async fn seed_ring(h: &Harness) {
    for id in ["r1", "r2", "r3"] {
        h.locations.save(106.8, -6.2, id).await.unwrap();
    }
    // Stale user: last_seen far in the past.
    h.locations
        .last_seen
        .lock()
        .unwrap()
        .insert("stale".into(), NOW - 10 * 60 * 1000);
    h.locations.rings.lock().unwrap().insert(
        1000,
        vec![
            "sender".into(),
            "r1".into(),
            "r2".into(),
            "r3".into(),
            "stale".into(),
        ],
    );
}

#[tokio::test]
async fn trigger_creates_the_alert_and_fans_out_to_fresh_users() {
    let h = Harness::new(Harness::zero_grace());
    seed_ring(&h).await;

    let created = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();

    assert_eq!(created.status, "TRIGGERED");
    assert_eq!(created.recipients_notified, 3);
    assert_eq!(created.triggered_at, NOW);
    assert_eq!(created.grace_period_seconds, 0);

    let alert = h
        .alerts
        .get_by_id(&created.alert_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alert.sender_id, "sender");
    assert_eq!(alert.emergency_type, "DANGER");
    assert_eq!(alert.dispatch_due_at, None);

    let recipients = h.alerts.list_recipients(&alert.id).await.unwrap();
    assert_eq!(recipients.len(), 3);
    assert!(!recipients.contains(&"stale".to_string()));
    assert!(!recipients.contains(&"sender".to_string()));

    // No grace period: the fan-out already ran.
    assert_eq!(h.notifier.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn trigger_falls_back_to_the_last_heartbeat() {
    let h = Harness::new(Harness::zero_grace());
    h.locations.save(106.8, -6.2, "sender").await.unwrap();

    let created = h
        .case
        .trigger("sender", TriggerSosRequest::default(), None)
        .await
        .unwrap();
    let alert = h
        .alerts
        .get_by_id(&created.alert_id)
        .await
        .unwrap()
        .unwrap();
    assert!((alert.latitude + 6.2).abs() < 1e-9);
    assert!((alert.longitude - 106.8).abs() < 1e-9);
}

#[tokio::test]
async fn trigger_without_any_location_is_unprocessable() {
    let h = Harness::new(Harness::zero_grace());
    let err = h
        .case
        .trigger("sender", TriggerSosRequest::default(), None)
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::Unprocessable(_)), "got {err:?}");
    assert_eq!(err.status(), 422);
}

#[tokio::test]
async fn latitude_without_longitude_is_bad_request() {
    let h = Harness::new(Harness::zero_grace());
    let request = TriggerSosRequest {
        latitude: Some(-6.2),
        ..Default::default()
    };
    assert!(matches!(
        h.case.trigger("sender", request, None).await.unwrap_err(),
        AppError::BadRequest(_)
    ));
}

#[tokio::test]
async fn idempotent_retry_replays_and_replays_before_the_limiter() {
    let h = Harness::new(Harness::zero_grace());
    seed_ring(&h).await;

    let first = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), Some("key-1"))
        .await
        .unwrap();

    // The limiter now blocks every new trigger.
    *h.limiter.allow.lock().unwrap() = false;
    let calls_before = h.limiter.calls.load(Ordering::SeqCst);

    let replay = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), Some("key-1"))
        .await
        .unwrap();

    assert_eq!(replay.alert_id, first.alert_id);
    assert_eq!(replay.recipients_notified, first.recipients_notified);
    assert_eq!(
        h.limiter.calls.load(Ordering::SeqCst),
        calls_before,
        "a replay must not touch the limiter"
    );
    assert_eq!(h.alerts.rows.lock().unwrap().len(), 1);

    // A different key is a new trigger and hits the limiter.
    let err = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), Some("key-2"))
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::TooManyRequests { .. }));
    assert_eq!(err.status(), 429);
}

#[tokio::test]
async fn failed_trigger_releases_its_idempotency_claim() {
    let h = Harness::new(Harness::zero_grace());
    // No heartbeat and no body coordinates: the trigger fails after claiming.
    let err = h
        .case
        .trigger("sender", TriggerSosRequest::default(), Some("retry-me"))
        .await
        .unwrap_err();
    assert_eq!(err.status(), 422);
    assert_eq!(h.idempotency.releases.load(Ordering::SeqCst), 1);

    // The retry is not a 409, it proceeds (and fails the same way).
    let again = h
        .case
        .trigger("sender", TriggerSosRequest::default(), Some("retry-me"))
        .await
        .unwrap_err();
    assert_eq!(again.status(), 422);
}

#[tokio::test]
async fn rate_limiter_failure_fails_open() {
    let h = Harness::new(Harness::zero_grace());
    seed_ring(&h).await;
    *h.limiter.fail.lock().unwrap() = true;

    let created = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();
    assert_eq!(created.recipients_notified, 3);
}

#[tokio::test]
async fn rate_limit_disabled_skips_the_limiter() {
    let h = Harness::new(SosSettings {
        rate_limit_disabled: true,
        ..Harness::zero_grace()
    });
    seed_ring(&h).await;

    h.case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();
    assert_eq!(h.limiter.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn recipient_lookup_failure_is_fail_open() {
    let h = Harness::new(Harness::zero_grace());
    *h.locations.fail_search.lock().unwrap() = true;

    let created = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();

    assert_eq!(created.recipients_notified, 0);
    // The alert exists and the notifier still ran (emergency-contact path).
    assert_eq!(h.alerts.rows.lock().unwrap().len(), 1);
    assert_eq!(h.notifier.calls.lock().unwrap().len(), 1);
    assert!(h.notifier.calls.lock().unwrap()[0].0.is_empty());
}

#[tokio::test]
async fn adaptive_radius_widens_until_the_minimum_is_reached() {
    let h = Harness::new(Harness::zero_grace());
    for id in ["a", "b", "c"] {
        h.locations.save(106.9, -6.3, id).await.unwrap();
    }
    h.locations.rings.lock().unwrap().insert(1000, Vec::new());
    h.locations
        .rings
        .lock()
        .unwrap()
        .insert(2000, vec!["a".into(), "b".into(), "c".into()]);

    let created = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();
    assert_eq!(created.recipients_notified, 3);
}

#[tokio::test]
async fn grace_period_defers_the_fan_out_to_the_outbox() {
    let h = Harness::new(SosSettings {
        grace_period: Duration::from_secs(10),
        ..Harness::zero_grace()
    });
    seed_ring(&h).await;

    let created = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();

    assert_eq!(created.grace_period_seconds, 10);
    assert_eq!(
        h.notifier.calls.lock().unwrap().len(),
        0,
        "dispatched inline"
    );

    let alert = h
        .alerts
        .get_by_id(&created.alert_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alert.dispatch_due_at, Some(NOW + 10_000));

    // Not due yet.
    assert_eq!(h.case.dispatch_due(100).await.unwrap(), 0);

    // Pretend the clock moved past the grace period.
    h.alerts
        .rows
        .lock()
        .unwrap()
        .get_mut(&created.alert_id)
        .unwrap()
        .dispatch_due_at = Some(NOW);
    let dispatched = h.case.dispatch_due(100).await.unwrap();
    assert_eq!(dispatched, 1);
    assert_eq!(h.notifier.calls.lock().unwrap().len(), 1);

    let alert = h
        .alerts
        .get_by_id(&created.alert_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alert.dispatched_at, Some(NOW));

    // A second scan does not re-dispatch: dispatched_at is a latch.
    assert_eq!(h.case.dispatch_due(100).await.unwrap(), 0);
    assert_eq!(h.notifier.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn outbox_skips_a_cancelled_alert_but_marks_it() {
    let h = Harness::new(SosSettings {
        grace_period: Duration::from_secs(10),
        ..Harness::zero_grace()
    });
    seed_ring(&h).await;
    let created = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();

    // Cancel during the grace window.
    h.case
        .cancel(
            "sender",
            &created.alert_id,
            CancelSosRequest {
                pin: "1234".into(),
                reason: String::new(),
            },
        )
        .await
        .unwrap();

    h.alerts
        .rows
        .lock()
        .unwrap()
        .get_mut(&created.alert_id)
        .unwrap()
        .dispatch_due_at = Some(NOW);

    // Cancelled alerts are not in the TRIGGERED scan at all.
    assert_eq!(h.case.dispatch_due(100).await.unwrap(), 0);
    assert_eq!(h.notifier.calls.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn cancel_checks_owner_pin_and_transition() {
    let h = Harness::new(Harness::zero_grace());
    seed_ring(&h).await;
    let created = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();

    // Not the owner.
    let err = h
        .case
        .cancel(
            "someone-else",
            &created.alert_id,
            CancelSosRequest {
                pin: "1234".into(),
                reason: String::new(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status(), 403);

    // Wrong PIN.
    let err = h
        .case
        .cancel(
            "sender",
            &created.alert_id,
            CancelSosRequest {
                pin: "9999".into(),
                reason: String::new(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status(), 403);

    // Correct PIN.
    let response = h
        .case
        .cancel(
            "sender",
            &created.alert_id,
            CancelSosRequest {
                pin: "1234".into(),
                reason: String::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(response.status, "CANCELLED");
    assert_eq!(response.cancelled_at, NOW);

    let alert = h
        .alerts
        .get_by_id(&created.alert_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alert.cancellation_reason.as_deref(), Some("FALSE_ALARM"));

    // A second cancel is a conflict.
    let err = h
        .case
        .cancel(
            "sender",
            &created.alert_id,
            CancelSosRequest {
                pin: "1234".into(),
                reason: String::new(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status(), 409);

    // The status change was broadcast.
    assert!(h
        .hub
        .published
        .lock()
        .unwrap()
        .iter()
        .any(|(_, payload)| payload.contains("ALERT_STATUS") && payload.contains("CANCELLED")));
}

#[tokio::test]
async fn cancel_without_a_configured_pin_is_precondition_failed() {
    let h = Harness::new(Harness::zero_grace());
    *h.users.pin_hash.lock().unwrap() = None;
    seed_ring(&h).await;
    let created = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();

    let err = h
        .case
        .cancel(
            "sender",
            &created.alert_id,
            CancelSosRequest {
                pin: "1234".into(),
                reason: String::new(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status(), 412);
}

#[tokio::test]
async fn details_are_masked_for_recipients_until_they_respond() {
    let h = Harness::new(Harness::zero_grace());
    seed_ring(&h).await;
    let created = h
        .case
        .trigger("sender", with_coords(-6.20049, 106.80049), None)
        .await
        .unwrap();

    // The sender sees exact coordinates.
    let sender_view = h.case.get_alert("sender", &created.alert_id).await.unwrap();
    assert!(!sender_view.masked);
    assert!((sender_view.initial_location.latitude + 6.20049).abs() < 1e-9);
    assert_eq!(sender_view.sender.name, "Sari");

    // A recipient who has not answered sees a ~100 m mask.
    let recipient_view = h.case.get_alert("r1", &created.alert_id).await.unwrap();
    assert!(recipient_view.masked);
    assert_eq!(recipient_view.initial_location.latitude, -6.2);
    assert_eq!(recipient_view.initial_location.longitude, 106.8);
    assert!(recipient_view.responders.is_none());

    // A stranger gets 404, not 403.
    let err = h
        .case
        .get_alert("stranger", &created.alert_id)
        .await
        .unwrap_err();
    assert_eq!(err.status(), 404);
}

#[tokio::test]
async fn respond_records_distance_eta_and_acknowledges() {
    let h = Harness::new(Harness::zero_grace());
    seed_ring(&h).await;
    let created = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();

    // ~1.1 km north of the victim.
    let response = h
        .case
        .respond(
            "r1",
            &created.alert_id,
            RespondSosRequest {
                responder_latitude: Some(-6.19),
                responder_longitude: Some(106.8),
            },
        )
        .await
        .unwrap();

    assert!(response.victim_distance_meters >= 1000);
    assert!(response.eta_seconds >= 60);

    let alert = h
        .alerts
        .get_by_id(&created.alert_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alert.status, "ACKNOWLEDGED");

    // The responder now sees exact coordinates and the roster.
    let view = h.case.get_alert("r1", &created.alert_id).await.unwrap();
    assert!(!view.masked);
    assert_eq!(view.responders_en_route, 1);
    assert_eq!(view.responders.unwrap().len(), 1);

    // A second answer is a conflict.
    let err = h
        .case
        .respond(
            "r1",
            &created.alert_id,
            RespondSosRequest {
                responder_latitude: Some(-6.19),
                responder_longitude: Some(106.8),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status(), 409);

    // The sender cannot respond to their own alert.
    let err = h
        .case
        .respond(
            "sender",
            &created.alert_id,
            RespondSosRequest {
                responder_latitude: Some(-6.19),
                responder_longitude: Some(106.8),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status(), 403);

    // A stranger cannot respond.
    let err = h
        .case
        .respond(
            "stranger",
            &created.alert_id,
            RespondSosRequest {
                responder_latitude: Some(-6.19),
                responder_longitude: Some(106.8),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status(), 403);
}

#[tokio::test]
async fn breadcrumbs_reject_old_clamp_future_and_broadcast() {
    let h = Harness::new(Harness::zero_grace());
    seed_ring(&h).await;
    let created = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();

    let fresh = h
        .case
        .add_breadcrumb(
            "sender",
            &created.alert_id,
            BreadcrumbRequest {
                latitude: Some(-6.19),
                longitude: Some(106.81),
                timestamp: Some(NOW - 5_000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(fresh.recorded_at, NOW - 5_000);

    // Older than 24 h is a client error.
    let err = h
        .case
        .add_breadcrumb(
            "sender",
            &created.alert_id,
            BreadcrumbRequest {
                latitude: Some(-6.19),
                longitude: Some(106.81),
                timestamp: Some(NOW - 25 * 3600 * 1000),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.status(), 400);

    // Far-future is clamped to now so it cannot freeze latest_location.
    let clamped = h
        .case
        .add_breadcrumb(
            "sender",
            &created.alert_id,
            BreadcrumbRequest {
                latitude: Some(-6.185),
                longitude: Some(106.812),
                timestamp: Some(NOW + 3600 * 1000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(clamped.recorded_at, NOW);

    // The Redis heartbeat is refreshed and the tracking hub was notified.
    assert!(h.locations.saves.load(Ordering::SeqCst) >= 3);
    assert!(h
        .hub
        .published
        .lock()
        .unwrap()
        .iter()
        .any(|(_, payload)| payload.contains("LOCATION_UPDATE")));

    let crumbs = h
        .case
        .list_breadcrumbs("sender", &created.alert_id)
        .await
        .unwrap();
    assert_eq!(crumbs.len(), 2);

    // Only participants may read the trail.
    let err = h
        .case
        .list_breadcrumbs("stranger", &created.alert_id)
        .await
        .unwrap_err();
    assert_eq!(err.status(), 404);
}

#[tokio::test]
async fn history_and_summary_cover_both_roles() {
    let h = Harness::new(Harness::zero_grace());
    seed_ring(&h).await;

    let first = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();
    h.case.resolve("sender", &first.alert_id).await.unwrap();
    h.case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();

    let (sent, total) = h
        .case
        .history(SearchAlertHistoryRequest {
            user_id: "sender".into(),
            role: "sent".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(total, 2);
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|row| row.responders.is_none()));

    let (received, total) = h
        .case
        .history(SearchAlertHistoryRequest {
            user_id: "r1".into(),
            role: "received".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(total, 2);
    assert_eq!(received.len(), 2);

    let summary = h.case.summary("sender").await.unwrap();
    assert_eq!(summary.sent.total, 2);
    assert_eq!(summary.sent.active, 1);
    assert_eq!(summary.sent.resolved, 1);
    assert_eq!(summary.received.total, 0);

    // Filter by status.
    let (resolved, total) = h
        .case
        .history(SearchAlertHistoryRequest {
            user_id: "sender".into(),
            role: "sent".into(),
            status: "closed".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(total, 1);
    assert_eq!(resolved[0].status, "RESOLVED");
}

#[tokio::test]
async fn triage_is_absent_and_client_claim_is_echoed_only() {
    let h = Harness::new(Harness::zero_grace());
    seed_ring(&h).await;

    let mut request = with_coords(-6.2, 106.8);
    request.client_severity = Some(0.9);
    request.client_summary = "pelapor bilang parah".into();

    let created = h.case.trigger("sender", request, None).await.unwrap();
    // No AI provider wired: no server triage is invented.
    assert!(created.triage.is_none());
    assert_eq!(created.client_severity, Some(0.9));
    assert_eq!(created.client_summary, "pelapor bilang parah");

    let alert = h
        .alerts
        .get_by_id(&created.alert_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        alert.severity.is_none(),
        "client claim leaked into alert.severity"
    );
    assert!(alert.summary.is_none());
}

#[test]
fn triage_type_is_serialized_only_when_present() {
    let empty = TriageInfo::default();
    let json = serde_json::to_value(&empty).unwrap();
    assert_eq!(json["severity"], 0.0);
    assert!(json.get("emergency_type").is_none());
}

#[tokio::test]
async fn triage_classifies_the_description_and_widens_the_start_radius() {
    let h = Harness::new(Harness::zero_grace()).with_triage();
    // Three fresh users at 1 km and three more at 2 km.
    for id in ["near-1", "near-2", "near-3", "far-1", "far-2", "far-3"] {
        h.locations.save(106.9, -6.3, id).await.unwrap();
    }
    h.locations.rings.lock().unwrap().insert(
        1000,
        vec!["near-1".into(), "near-2".into(), "near-3".into()],
    );
    // Deliberately only the far ring here: if the code started at 1 km the
    // near users would be picked and no recipient would come from 2 km.
    h.locations
        .rings
        .lock()
        .unwrap()
        .insert(2000, vec!["far-1".into(), "far-2".into(), "far-3".into()]);

    // "kecelakaan, teman saya pingsan" -> MEDICAL at 0.6, which starts the
    // fan-out at the 2 km ring (index 1 of 3).
    let request = TriggerSosRequest {
        latitude: Some(-6.2),
        longitude: Some(106.8),
        description: "kecelakaan, teman saya pingsan".into(),
        ..Default::default()
    };
    let created = h.case.trigger("sender", request, None).await.unwrap();

    let triage = created.triage.expect("triage attached");
    assert_eq!(triage.emergency_type, "MEDICAL");
    assert!((triage.severity - 0.6).abs() < 1e-9);
    assert!(!triage.summary.is_empty());

    let alert = h
        .alerts
        .get_by_id(&created.alert_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alert.emergency_type, "MEDICAL");
    assert_eq!(alert.severity, Some(0.6));
    assert!(alert.summary.is_some());

    // The 2 km ring was searched first: the far users are recipients even
    // though three fresh users sat inside 1 km.
    let recipients = h.alerts.list_recipients(&created.alert_id).await.unwrap();
    assert!(recipients.contains(&"far-1".to_string()));
    assert!(!recipients.contains(&"near-1".to_string()));
}

#[tokio::test]
async fn explicit_emergency_type_wins_over_triage_but_severity_still_applies() {
    let h = Harness::new(Harness::zero_grace()).with_triage();
    seed_ring(&h).await;

    let request = TriggerSosRequest {
        latitude: Some(-6.2),
        longitude: Some(106.8),
        emergency_type: "ACCIDENT".into(),
        description: "kebakaran di rumah tetangga".into(),
        ..Default::default()
    };
    let created = h.case.trigger("sender", request, None).await.unwrap();

    let alert = h
        .alerts
        .get_by_id(&created.alert_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alert.emergency_type, "ACCIDENT", "request type must win");
    assert_eq!(alert.severity, Some(0.8), "triage severity still stored");
}

#[tokio::test]
async fn no_description_means_no_triage() {
    let h = Harness::new(Harness::zero_grace()).with_triage();
    seed_ring(&h).await;

    let created = h
        .case
        .trigger("sender", with_coords(-6.2, 106.8), None)
        .await
        .unwrap();
    assert!(created.triage.is_none());
    let alert = h
        .alerts
        .get_by_id(&created.alert_id)
        .await
        .unwrap()
        .unwrap();
    assert!(alert.severity.is_none());
    assert_eq!(alert.emergency_type, "DANGER");
}

#[test]
fn media_filenames_are_sanitized_like_go() {
    use crate::usecase::sos::sanitize_filename;

    assert_eq!(sanitize_filename("cry.wav").unwrap(), "cry.wav");
    // Directory components are dropped.
    assert_eq!(sanitize_filename("../../etc/passwd").unwrap(), "passwd");
    assert_eq!(sanitize_filename("C:\\temp\\cry.wav").unwrap(), "cry.wav");
    // Control characters removed.
    assert_eq!(sanitize_filename("cr\u{0}y.wav").unwrap(), "cry.wav");
    // Nothing usable left.
    assert!(sanitize_filename("").is_err());
    assert!(sanitize_filename("..").is_err());

    // Long names keep their extension.
    let long = format!("{}.wav", "a".repeat(400));
    let sanitized = sanitize_filename(&long).unwrap();
    assert_eq!(sanitized.chars().count(), 200);
    assert!(sanitized.ends_with(".wav"), "got {sanitized}");
}
