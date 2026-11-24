//! Provider and dispatcher behaviour against a local mock HTTP server.
//!
//! Nothing here talks to Google, Apple or Twilio: every request shape is
//! asserted on the wire, including auth headers, so a config mistake shows up
//! in CI instead of in production.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, Uri};
use axum::routing::any;
use axum::Router;

use crate::config::{ApnsConfig, FcmConfig, TwilioConfig};
use crate::domain::{EmergencyContact, User};
use crate::error::{AppError, AppResult};
use crate::model::sos::AlertNotice;
use crate::usecase::auth::UserStore;
use crate::usecase::emergency::EmergencyContactStore;
use crate::usecase::sos::SosNotifier;

use super::dispatcher::AlertDispatcher;
use super::{ApnsNotifier, FcmNotifier, ProviderError, TwilioNotifier};

/// A throwaway P-256 PKCS#8 key. Test-only: it protects nothing.
const TEST_APNS_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgUWDUT3ZC8xMsC0UN
m8jhY/UbYmc9m5fiFX0F5IFoRJ+hRANCAATY8jMO/tHK2alY9+qg5FlmN2ADRd+X
2+j4fMrRH4NjPylRHE1aYL+FdV3SOUt19cPScMLDkxQNEAKWED77ic0Z
-----END PRIVATE KEY-----";

#[derive(Debug, Clone, Default)]
struct CapturedRequest {
    method: Method,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<CapturedRequest>>>);

impl Capture {
    fn requests(&self) -> Vec<CapturedRequest> {
        self.0.lock().unwrap().clone()
    }
}

async fn record(
    State(capture): State<Capture>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> &'static str {
    let mut header_map = HashMap::new();
    for (name, value) in headers.iter() {
        header_map.insert(
            name.as_str().to_owned(),
            value.to_str().unwrap_or_default().to_owned(),
        );
    }
    capture.0.lock().unwrap().push(CapturedRequest {
        method,
        path: uri.path().to_owned(),
        headers: header_map,
        body: body.to_vec(),
    });
    "{}"
}

/// Start a capture server and return `(base_url, capture)`.
async fn mock_server() -> (String, Capture) {
    let capture = Capture::default();
    let app = Router::new()
        .fallback(any(record))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), capture)
}

fn fcm_config() -> FcmConfig {
    FcmConfig {
        enabled: true,
        project_id: "sos-project".into(),
        access_token: "fcm-token".into(),
    }
}

fn apns_config() -> ApnsConfig {
    ApnsConfig {
        enabled: true,
        key_id: "KEYID12345".into(),
        team_id: "TEAM123456".into(),
        bundle_id: "id.allert.app".into(),
        private_key_path: String::new(),
        production: false,
    }
}

fn twilio_config() -> TwilioConfig {
    TwilioConfig {
        enabled: true,
        account_sid: "AC123".into(),
        auth_token: "twilio-secret".into(),
        from_number: "+628111".into(),
    }
}

fn data() -> HashMap<String, String> {
    let mut map = HashMap::new();
    map.insert("alert_id".into(), "alert-1".into());
    map
}

#[tokio::test]
async fn fcm_token_request_has_the_go_payload_and_bearer_auth() {
    let (base, capture) = mock_server().await;
    let fcm = FcmNotifier::new(&fcm_config()).with_base_url(&base);

    fcm.send_to_token("device-1", "SOS Alert", "help", &data())
        .await
        .unwrap();

    let requests = capture.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.method, Method::POST);
    assert_eq!(request.path, "/v1/projects/sos-project/messages:send");
    assert_eq!(
        request.headers.get("authorization").map(String::as_str),
        Some("Bearer fcm-token")
    );

    let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    let message = &body["message"];
    assert_eq!(message["token"], "device-1");
    assert!(
        message.get("topic").is_none(),
        "token and topic are exclusive"
    );
    assert_eq!(message["notification"]["title"], "SOS Alert");
    assert_eq!(message["notification"]["body"], "help");
    assert_eq!(message["android"]["priority"], "HIGH");
    assert_eq!(
        message["android"]["notification"]["channel_id"],
        "sos_alerts"
    );
    assert_eq!(message["android"]["notification"]["sound"], "siren");
    assert_eq!(message["android"]["notification"]["default_sound"], true);
    assert_eq!(message["apns"]["payload"]["aps"]["sound"]["critical"], 1);
    assert_eq!(
        message["apns"]["payload"]["aps"]["interruption-level"],
        "critical"
    );
    assert_eq!(message["data"]["alert_id"], "alert-1");
}

#[tokio::test]
async fn fcm_without_a_token_targets_the_per_user_topic() {
    let (base, capture) = mock_server().await;
    let fcm = FcmNotifier::new(&fcm_config()).with_base_url(&base);

    fcm.send_to_topic("user_abc", "t", "b", &data())
        .await
        .unwrap();

    let body: serde_json::Value = serde_json::from_slice(&capture.requests()[0].body).unwrap();
    assert_eq!(body["message"]["topic"], "user_abc");
    assert!(body["message"].get("token").is_none());
}

#[tokio::test]
async fn unconfigured_fcm_never_reaches_the_network() {
    let (base, capture) = mock_server().await;
    let mut config = fcm_config();
    config.enabled = false;
    let fcm = FcmNotifier::new(&config).with_base_url(&base);

    assert_eq!(
        fcm.send_to_token("d", "t", "b", &data()).await,
        Err(ProviderError::NotConfigured)
    );
    assert!(capture.requests().is_empty());
}

#[tokio::test]
async fn apns_sends_a_critical_alert_with_the_required_headers() {
    let (base, capture) = mock_server().await;
    let apns =
        ApnsNotifier::new(&apns_config(), TEST_APNS_KEY.as_bytes().to_vec()).with_base_url(&base);

    apns.send("device-token", "SOS Alert", "help", "alert-1")
        .await
        .unwrap();

    let requests = capture.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.method, Method::POST);
    assert_eq!(request.path, "/3/device/device-token");
    assert_eq!(
        request.headers.get("apns-topic").map(String::as_str),
        Some("id.allert.app")
    );
    assert_eq!(
        request.headers.get("apns-push-type").map(String::as_str),
        Some("alert")
    );
    assert_eq!(
        request.headers.get("apns-priority").map(String::as_str),
        Some("10")
    );
    assert_eq!(
        request.headers.get("apns-expiration").map(String::as_str),
        Some("0")
    );

    let authorization = request.headers.get("authorization").unwrap();
    let token = authorization.strip_prefix("Bearer ").expect("bearer token");
    assert_eq!(token.split('.').count(), 3, "JWT must have three parts");

    let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(body["aps"]["alert"]["title"], "SOS Alert");
    assert_eq!(body["aps"]["sound"]["critical"], 1);
    assert_eq!(body["aps"]["interruption-level"], "critical");
    assert_eq!(body["aps"]["category"], "SOS_ALERT");
    assert_eq!(body["alert_id"], "alert-1");
}

#[tokio::test]
async fn apns_provider_token_is_cached_between_sends() {
    let notifier = ApnsNotifier::new(&apns_config(), TEST_APNS_KEY.as_bytes().to_vec());
    let first = notifier.provider_token().unwrap();
    let second = notifier.provider_token().unwrap();
    assert_eq!(first, second, "one JWT per fan-out, not per device");
}

#[tokio::test]
async fn an_unparsable_apns_key_is_reported() {
    let notifier = ApnsNotifier::new(&apns_config(), b"not a pem".to_vec());
    assert!(matches!(
        notifier.provider_token(),
        Err(ProviderError::Failed(_))
    ));
}

#[tokio::test]
async fn twilio_posts_a_form_with_basic_auth() {
    let (base, capture) = mock_server().await;
    let twilio = TwilioNotifier::new(&twilio_config()).with_base_url(&base);

    twilio.send_sms("+628123", "SOS ALERT").await.unwrap();

    let requests = capture.requests();
    let request = &requests[0];
    assert_eq!(request.method, Method::POST);
    assert_eq!(request.path, "/2010-04-01/Accounts/AC123/Messages.json");
    let authorization = request.headers.get("authorization").unwrap();
    assert!(authorization.starts_with("Basic "), "got {authorization}");
    let content_type = request.headers.get("content-type").unwrap();
    assert!(content_type.starts_with("application/x-www-form-urlencoded"));

    let body = String::from_utf8_lossy(&request.body);
    assert!(body.contains("To=%2B628123"), "body: {body}");
    assert!(body.contains("From=%2B628111"), "body: {body}");
    assert!(body.contains("Body=SOS+ALERT"), "body: {body}");
}

#[tokio::test]
async fn an_unconfigured_twilio_is_not_configured_not_an_error() {
    let (base, capture) = mock_server().await;
    let mut config = twilio_config();
    config.auth_token = String::new();
    let twilio = TwilioNotifier::new(&config).with_base_url(&base);
    assert_eq!(
        twilio.send_sms("+628123", "x").await,
        Err(ProviderError::NotConfigured)
    );
    assert!(capture.requests().is_empty());
}

#[test]
fn notification_content_labels_the_senders_claim() {
    let notice = AlertNotice {
        alert_id: "alert-1".into(),
        sender_id: "sender-1".into(),
        status: "TRIGGERED".into(),
        emergency_type: "MEDICAL".into(),
        severity: Some(0.6),
        summary: "Kemungkinan darurat medis (pingsan)".into(),
        client_severity: Some(0.9),
        client_summary: "parah sekali".into(),
        latitude: -6.2,
        longitude: 106.8,
        triggered_at: 1,
    };

    let (title, body, data) = AlertDispatcher::notification_content(&notice);
    assert_eq!(title, "SOS Alert");
    // The sender's claim is prefixed, then the server summary, then the
    // generic line: same order as the Go notificationContent.
    assert!(
        body.starts_with("Pelapor menyebut: parah sekali — "),
        "body: {body}"
    );
    assert!(body.contains("Kemungkinan darurat medis (pingsan) — "));

    assert!(body.contains("Emergency MEDICAL nearby"));
    assert_eq!(data["severity"], "0.60");
    assert_eq!(data["client_severity"], "0.90");
    assert_eq!(data["latitude"], "-6.200000");
    assert_eq!(data["longitude"], "106.800000");
}

#[test]
fn notification_content_without_a_claim_is_plain() {
    let notice = AlertNotice {
        alert_id: "a".into(),
        sender_id: "s".into(),
        status: "TRIGGERED".into(),
        emergency_type: String::new(),
        severity: None,
        summary: String::new(),
        client_severity: None,
        client_summary: String::new(),
        latitude: 0.0,
        longitude: 0.0,
        triggered_at: 1,
    };
    let (_, body, data) = AlertDispatcher::notification_content(&notice);
    assert_eq!(
        body,
        "Emergency alert nearby. Open the app for the live location."
    );
    assert!(!data.contains_key("severity"));
    assert!(!data.contains_key("client_summary"));
}

struct FakeTokens {
    user: User,
}

#[async_trait]
impl UserStore for FakeTokens {
    async fn count_by_id(&self, _id: &str) -> AppResult<i64> {
        Ok(1)
    }
    async fn find_by_id(&self, _id: &str) -> AppResult<Option<User>> {
        Ok(Some(self.user.clone()))
    }
    async fn create(&self, _user: &User) -> AppResult<()> {
        Ok(())
    }
    async fn update(&self, _user: &User) -> AppResult<()> {
        Ok(())
    }
    async fn names_by_ids(&self, _ids: &[String]) -> AppResult<HashMap<String, String>> {
        Ok(HashMap::new())
    }
}

struct FakeContacts {
    rows: Vec<EmergencyContact>,
}

#[async_trait]
impl EmergencyContactStore for FakeContacts {
    async fn create(&self, _contact: &EmergencyContact) -> AppResult<()> {
        Ok(())
    }
    async fn list_by_user(&self, _user_id: &str) -> AppResult<Vec<EmergencyContact>> {
        Ok(self.rows.clone())
    }
}

fn user_with_tokens() -> User {
    User {
        id: "r1".into(),
        name: "Responder".into(),
        password: String::new(),
        created_at: 1,
        updated_at: 1,
        safe_pin_hash: None,
        fcm_token: Some("fcm-device".into()),
        apns_token: Some("apns-device".into()),
    }
}

fn contact() -> EmergencyContact {
    EmergencyContact {
        id: "c1".into(),
        user_id: "sender-1".into(),
        contact_name: "Ibu".into(),
        phone_number: "+6281234567890".into(),
        relationship: "family".into(),
        is_verified: true,
        created_at: 1,
    }
}

fn notice() -> AlertNotice {
    AlertNotice {
        alert_id: "alert-1".into(),
        sender_id: "sender-1".into(),
        status: "TRIGGERED".into(),
        emergency_type: "MEDICAL".into(),
        severity: Some(0.6),
        summary: "pingsan".into(),
        client_severity: None,
        client_summary: String::new(),
        latitude: -6.2,
        longitude: 106.8,
        triggered_at: 1,
    }
}

fn dispatcher(base: &str, with_contacts: bool) -> AlertDispatcher {
    AlertDispatcher {
        fcm: Some(FcmNotifier::new(&fcm_config()).with_base_url(base)),
        apns: Some(
            ApnsNotifier::new(&apns_config(), TEST_APNS_KEY.as_bytes().to_vec())
                .with_base_url(base),
        ),
        twilio: Some(TwilioNotifier::new(&twilio_config()).with_base_url(base)),
        tokens: Some(Arc::new(FakeTokens {
            user: user_with_tokens(),
        })),
        contacts: with_contacts.then(|| {
            Arc::new(FakeContacts {
                rows: vec![contact()],
            }) as Arc<dyn EmergencyContactStore>
        }),
    }
}

#[tokio::test]
async fn notify_pushes_to_both_devices_and_texts_the_emergency_contact() {
    let (base, capture) = mock_server().await;
    let dispatcher = dispatcher(&base, true);

    dispatcher
        .notify(&["r1".to_string()], &notice())
        .await
        .unwrap();

    let requests = capture.requests();
    let paths: Vec<&str> = requests.iter().map(|r| r.path.as_str()).collect();
    assert!(
        paths.contains(&"/v1/projects/sos-project/messages:send"),
        "paths: {paths:?}"
    );
    assert!(paths.contains(&"/3/device/apns-device"), "paths: {paths:?}");
    assert!(
        paths.contains(&"/2010-04-01/Accounts/AC123/Messages.json"),
        "paths: {paths:?}"
    );

    let sms = requests
        .iter()
        .find(|r| r.path.contains("/Messages.json"))
        .unwrap();
    let body = String::from_utf8_lossy(&sms.body);
    assert!(body.contains("SOS+ALERT+%28MEDICAL%29"), "body: {body}");
    assert!(
        body.contains("google.com%2Fmaps%2Fsearch"),
        "the maps link must be in the SMS: {body}"
    );
    assert!(body.contains("-6.200000%2C106.800000"), "body: {body}");
}

#[tokio::test]
async fn notify_recipients_never_texts_the_emergency_contacts() {
    let (base, capture) = mock_server().await;
    let dispatcher = dispatcher(&base, true);

    dispatcher
        .notify_recipients(&["r1".to_string()], &notice())
        .await
        .unwrap();

    let paths: Vec<String> = capture.requests().into_iter().map(|r| r.path).collect();
    assert!(
        !paths.iter().any(|p| p.contains("/Messages.json")),
        "an escalation must not text: {paths:?}"
    );
    assert!(paths.iter().any(|p| p.contains("messages:send")));
}

#[tokio::test]
async fn an_unconfigured_dispatcher_is_log_only_and_succeeds() {
    let dispatcher = AlertDispatcher {
        fcm: None,
        apns: None,
        twilio: None,
        tokens: None,
        contacts: Some(Arc::new(FakeContacts {
            rows: vec![contact()],
        })),
    };

    dispatcher
        .notify(&["r1".to_string()], &notice())
        .await
        .unwrap();
}

#[tokio::test]
async fn a_failing_push_alone_still_succeeds_when_no_sms_is_configured() {
    // FCM points at a closed port: the request fails, but with no SMS channel
    // the dispatcher reports success so the alert is not retried forever.
    let dispatcher = AlertDispatcher {
        fcm: Some(FcmNotifier::new(&fcm_config()).with_base_url("http://127.0.0.1:1")),
        apns: None,
        twilio: None,
        tokens: Some(Arc::new(FakeTokens {
            user: user_with_tokens(),
        })),
        contacts: None,
    };

    dispatcher
        .notify(&["r1".to_string()], &notice())
        .await
        .unwrap();
}

#[tokio::test]
async fn a_failing_push_and_failing_sms_error() {
    struct FailingContacts;
    #[async_trait]
    impl EmergencyContactStore for FailingContacts {
        async fn create(&self, _contact: &EmergencyContact) -> AppResult<()> {
            Ok(())
        }
        async fn list_by_user(&self, _user_id: &str) -> AppResult<Vec<EmergencyContact>> {
            Err(AppError::Unavailable("contacts down".into()))
        }
    }

    let dispatcher = AlertDispatcher {
        fcm: Some(FcmNotifier::new(&fcm_config()).with_base_url("http://127.0.0.1:1")),
        apns: None,
        twilio: None,
        tokens: Some(Arc::new(FakeTokens {
            user: user_with_tokens(),
        })),
        contacts: Some(Arc::new(FailingContacts)),
    };

    assert!(dispatcher
        .notify(&["r1".to_string()], &notice())
        .await
        .is_err());
}
