//! Web Push: endpoint allowlist, subscription ownership and the encrypted send.
//!
//! The send path uses a fake transport, so every header and the ciphertext are
//! asserted without TLS, DNS or a live push service.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::config::WebPushConfig;
use crate::domain::PushSubscription;
use crate::error::{AppError, AppResult};
use crate::model::push::{PushSubscribeRequest, PushUnsubscribeRequest};
use crate::usecase::push::{
    hosts_or, is_allowed_endpoint, PushSubscriptionStore, PushUseCase, DEFAULT_WEB_PUSH_HOSTS,
};

use super::ProviderError;
use super::{PushTransport, WebPushNotifier};

/// Throwaway browser subscription keys (uncompressed P-256 point + 16-byte
/// auth secret) generated with openssl for this test only.
const SUB_P256DH: &str =
    "BBbPAKQouzcba6Pmc_x27ZrWGErXw927hBRW8VJugIyCF62Ph8pQjN9j3kpKU3he1wHY92bTAl9TmWtcI2BKoh0";
const SUB_AUTH: &str = "AAECAwQFBgcICQoLDA0ODw";

/// Throwaway VAPID keypair (public point + raw private scalar, base64url).
const VAPID_PUBLIC: &str =
    "BF1slwHKJE8AOcOh2SY_LPsCN3UhX0_T8l0q6iu5ylZNRzASaZbmZx2AmP64PmgF3xn627KYvU_mnAhH7TF9K3Y";
const VAPID_PRIVATE: &str = "cev4pqFs3hn-rX0ISjOT0OG2yKAFGF8WW967rv7D7WI";

fn subscription_keys() -> (String, String) {
    (SUB_P256DH.to_string(), SUB_AUTH.to_string())
}

#[test]
fn the_endpoint_allowlist_matches_the_go_rules() {
    let allowed = hosts_or(&[]);
    assert_eq!(allowed, DEFAULT_WEB_PUSH_HOSTS.to_vec());

    assert!(is_allowed_endpoint(
        "https://fcm.googleapis.com/fcm/send/abc",
        &allowed
    ));
    assert!(is_allowed_endpoint(
        "https://updates.push.services.mozilla.com/wpush/v2/xyz",
        &allowed
    ));
    // https is mandatory: the guard prevents a stored http SSRF target.
    assert!(!is_allowed_endpoint(
        "http://fcm.googleapis.com/fcm/send/abc",
        &allowed
    ));
    // A host that merely contains an allowed name is not allowed.
    assert!(!is_allowed_endpoint(
        "https://evil-fcm.googleapis.com.attacker.test/x",
        &allowed
    ));
    assert!(!is_allowed_endpoint(
        "https://127.0.0.1:9000/push",
        &allowed
    ));
    assert!(!is_allowed_endpoint("not a url", &allowed));

    // A configured suffix entry covers per-shard subdomains.
    let suffix = vec![".push.apple.com".to_string()];
    assert!(is_allowed_endpoint("https://web.push.apple.com/x", &suffix));
    assert!(is_allowed_endpoint(
        "https://shard.push.apple.com/x",
        &suffix
    ));
    assert!(!is_allowed_endpoint(
        "https://push.apple.com.evil/x",
        &suffix
    ));
}

#[derive(Default)]
struct FakeSubs {
    rows: Mutex<Vec<PushSubscription>>,
    /// endpoint -> owning user, the ownership rule under test.
    owners: Mutex<HashMap<String, String>>,
    deleted: Mutex<Vec<String>>,
    fail_list: AtomicBool,
}

#[async_trait]
impl PushSubscriptionStore for FakeSubs {
    async fn upsert(&self, subscription: &PushSubscription) -> AppResult<String> {
        let mut owners = self.owners.lock().unwrap();
        if let Some(current) = owners.get(&subscription.endpoint) {
            if current != &subscription.user_id {
                return Err(AppError::Conflict(
                    "push endpoint is already registered to another user".into(),
                ));
            }
            // Re-subscribe keeps the row id.
            let existing = self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|row| row.endpoint == subscription.endpoint)
                .map(|row| row.id.clone())
                .unwrap_or_else(|| subscription.id.clone());
            return Ok(existing);
        }
        owners.insert(subscription.endpoint.clone(), subscription.user_id.clone());
        self.rows.lock().unwrap().push(subscription.clone());
        Ok(subscription.id.clone())
    }

    async fn list_by_user(&self, user_id: &str) -> AppResult<Vec<PushSubscription>> {
        if self.fail_list.load(Ordering::SeqCst) {
            return Err(AppError::Unavailable("db down".into()));
        }
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row.user_id == user_id)
            .cloned()
            .collect())
    }

    async fn delete_by_user_and_endpoint(&self, user_id: &str, endpoint: &str) -> AppResult<()> {
        self.rows
            .lock()
            .unwrap()
            .retain(|row| !(row.user_id == user_id && row.endpoint == endpoint));
        Ok(())
    }

    async fn delete_by_endpoint(&self, endpoint: &str) -> AppResult<()> {
        self.deleted.lock().unwrap().push(endpoint.to_owned());
        self.rows
            .lock()
            .unwrap()
            .retain(|row| row.endpoint != endpoint);
        Ok(())
    }
}

fn request(endpoint: &str) -> PushSubscribeRequest {
    PushSubscribeRequest {
        endpoint: endpoint.into(),
        p256dh: "key".into(),
        auth: "auth".into(),
        user_agent: "ua".into(),
    }
}

fn usecase() -> (PushUseCase, Arc<FakeSubs>) {
    let subs = Arc::new(FakeSubs::default());
    let case = PushUseCase::new(
        subs.clone() as Arc<dyn PushSubscriptionStore>,
        "public-key".into(),
        true,
        Vec::new(),
    )
    .with_clock(Arc::new(|| 1_700_000_000_000));
    (case, subs)
}

#[tokio::test]
async fn subscribing_a_foreign_host_is_a_bad_request() {
    let (case, _) = usecase();
    let err = case
        .subscribe("user-1", request("http://169.254.169.254/latest/meta-data"))
        .await
        .unwrap_err();
    assert_eq!(err.status(), 400);
}

#[tokio::test]
async fn re_subscribing_the_same_endpoint_for_another_user_conflicts() {
    let (case, _) = usecase();
    case.subscribe("user-1", request("https://fcm.googleapis.com/fcm/send/a"))
        .await
        .unwrap();

    let err = case
        .subscribe("user-2", request("https://fcm.googleapis.com/fcm/send/a"))
        .await
        .unwrap_err();
    assert_eq!(err.status(), 409);
    assert!(err.to_string().contains("already registered"));
}

#[tokio::test]
async fn list_and_unsubscribe_are_scoped_to_the_user() {
    let (case, _) = usecase();
    case.subscribe("user-1", request("https://fcm.googleapis.com/fcm/send/a"))
        .await
        .unwrap();
    case.subscribe("user-2", request("https://fcm.googleapis.com/fcm/send/b"))
        .await
        .unwrap();

    assert_eq!(case.list("user-1").await.unwrap().len(), 1);
    assert!(case
        .unsubscribe(
            "user-1",
            PushUnsubscribeRequest {
                endpoint: "https://fcm.googleapis.com/fcm/send/a".into()
            }
        )
        .await
        .unwrap());
    assert!(case.list("user-1").await.unwrap().is_empty());
    assert_eq!(case.list("user-2").await.unwrap().len(), 1);
}

#[test]
fn the_vapid_key_is_503_when_web_push_is_off() {
    let subs = Arc::new(FakeSubs::default());
    let disabled = PushUseCase::new(
        subs.clone() as Arc<dyn PushSubscriptionStore>,
        "public".into(),
        false,
        Vec::new(),
    );
    let error = disabled.vapid_public_key().unwrap_err();
    assert_eq!(error.status(), 503);

    let empty = PushUseCase::new(
        subs as Arc<dyn PushSubscriptionStore>,
        String::new(),
        true,
        Vec::new(),
    );
    assert_eq!(empty.vapid_public_key().unwrap_err().status(), 503);
}

#[derive(Debug, Clone, Default)]
struct CapturedPush {
    endpoint: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

struct FakeTransport {
    status: u16,
    captured: Mutex<Vec<CapturedPush>>,
}

impl FakeTransport {
    fn new(status: u16) -> Arc<Self> {
        Arc::new(Self {
            status,
            captured: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl PushTransport for FakeTransport {
    async fn send(
        &self,
        endpoint: &str,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<u16, String> {
        self.captured.lock().unwrap().push(CapturedPush {
            endpoint: endpoint.to_owned(),
            headers,
            body,
        });
        Ok(self.status)
    }
}

fn webpush_config() -> WebPushConfig {
    WebPushConfig {
        enabled: true,
        vapid_public_key: VAPID_PUBLIC.to_string(),
        vapid_private_key: VAPID_PRIVATE.to_string(),
        subscriber: "sos@example.com".into(),
        ttl_seconds: 300,
        allowed_hosts: vec!["fcm.googleapis.com".into()],
    }
}

fn notifier(transport: Arc<FakeTransport>, subs: Arc<FakeSubs>, endpoint: &str) -> WebPushNotifier {
    let (p256dh, auth) = subscription_keys();
    subs.rows.lock().unwrap().push(PushSubscription {
        id: "sub-1".into(),
        user_id: "r1".into(),
        endpoint: endpoint.into(),
        p256dh,
        auth,
        user_agent: None,
        created_at: 1,
        updated_at: 1,
    });

    WebPushNotifier::new(&webpush_config(), subs as Arc<dyn PushSubscriptionStore>)
        .with_transport(transport)
}

#[tokio::test]
async fn a_send_is_encrypted_and_carries_the_vapid_headers() {
    let transport = FakeTransport::new(201);
    let notifier = notifier(
        transport.clone(),
        Arc::new(FakeSubs::default()),
        "https://fcm.googleapis.com/fcm/send/abc",
    );

    let mut data = HashMap::new();
    data.insert("alert_id".into(), "alert-1".into());
    notifier
        .send_to_user("r1", "SOS Alert", "help", &data)
        .await
        .unwrap();

    let captured = transport.captured.lock().unwrap();
    assert_eq!(captured.len(), 1);
    let push = &captured[0];
    assert_eq!(push.endpoint, "https://fcm.googleapis.com/fcm/send/abc");

    let header = |name: &str| {
        push.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
            .unwrap_or_default()
    };
    assert_eq!(header("TTL"), "300");
    assert_eq!(header("Urgency"), "high");
    assert_eq!(header("Content-Encoding"), "aes128gcm");
    assert!(header("Authorization").starts_with("vapid t="));
    // RFC 8291 body: salt(16) | rs(4) | idlen(1) | as_public(65) | ciphertext
    assert!(push.body.len() > 86, "encrypted body too short");
}

#[tokio::test]
async fn a_gone_endpoint_is_deleted() {
    let transport = FakeTransport::new(410);
    let subs = Arc::new(FakeSubs::default());
    let notifier = notifier(
        transport,
        subs.clone(),
        "https://fcm.googleapis.com/fcm/send/gone",
    );

    notifier
        .send_to_user("r1", "SOS Alert", "help", &HashMap::new())
        .await
        .unwrap();

    assert_eq!(
        subs.deleted.lock().unwrap().as_slice(),
        ["https://fcm.googleapis.com/fcm/send/gone"]
    );
}

#[tokio::test]
async fn a_non_allowlisted_stored_endpoint_is_dropped_without_sending() {
    let transport = FakeTransport::new(201);
    let subs = Arc::new(FakeSubs::default());
    // Stored before the allowlist shrank: must never be POSTed to.
    let notifier = notifier(
        transport.clone(),
        subs.clone(),
        "https://attacker.test/push",
    );

    notifier
        .send_to_user("r1", "SOS Alert", "help", &HashMap::new())
        .await
        .unwrap();

    assert!(transport.captured.lock().unwrap().is_empty());
    assert_eq!(
        subs.deleted.lock().unwrap().as_slice(),
        ["https://attacker.test/push"]
    );
}

#[tokio::test]
async fn a_500_from_the_push_service_is_an_error() {
    let transport = FakeTransport::new(500);
    let notifier = notifier(
        transport,
        Arc::new(FakeSubs::default()),
        "https://fcm.googleapis.com/fcm/send/abc",
    );

    let error = notifier
        .send_to_user("r1", "SOS Alert", "help", &HashMap::new())
        .await
        .unwrap_err();
    assert!(matches!(error, ProviderError::Failed(_)));
}

#[tokio::test]
async fn a_disabled_notifier_is_not_configured() {
    let mut config = webpush_config();
    config.enabled = false;
    let subs = Arc::new(FakeSubs::default());
    let notifier = WebPushNotifier::new(&config, subs as Arc<dyn PushSubscriptionStore>)
        .with_transport(FakeTransport::new(201));

    assert_eq!(
        notifier.send_to_user("r1", "t", "b", &HashMap::new()).await,
        Err(ProviderError::NotConfigured)
    );
}

#[test]
fn the_worker_payload_points_at_the_inbox() {
    let mut data = HashMap::new();
    data.insert("alert_id".into(), "alert-1".into());
    let payload = WebPushNotifier::build_payload("SOS Alert", "help", &data).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(json["title"], "SOS Alert");
    assert_eq!(json["body"], "help");
    assert_eq!(json["data"]["url"], "/");
    assert_eq!(json["data"]["alert_id"], "alert-1");
}

// ---------------------------------------------------------------------------
// RFC 8291 round trip: decrypt with the receiver's private key, independently
// of the notifier's code path.
// ---------------------------------------------------------------------------

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::Aes128Gcm;
use base64::Engine;
use hkdf::Hkdf;
use p256::ecdsa::signature::Verifier;
use p256::ecdsa::{Signature, VerifyingKey};
use p256::elliptic_curve::sec1::Sec1Point;
use p256::{PublicKey, SecretKey};
use sha2::Sha256;

fn b64(data: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
}

fn unb64(text: &str) -> Vec<u8> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(text)
        .unwrap()
}

fn uncompressed(public: &PublicKey) -> Vec<u8> {
    Sec1Point::<p256::NistP256>::from(*public)
        .as_bytes()
        .to_vec()
}

fn random_secret() -> SecretKey {
    for _ in 0..16 {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes).unwrap();
        if let Ok(secret) = SecretKey::from_slice(&bytes) {
            return secret;
        }
    }
    panic!("cannot generate a P-256 key");
}

/// Receiver side of RFC 8291 for a single record.
fn decrypt(body: &[u8], ua_secret: &SecretKey, auth_secret: &[u8], ua_public: &[u8]) -> Vec<u8> {
    // Body layout: salt(16) | rs(4) | idlen(1) | keyid | ciphertext.
    let salt = &body[..16];
    let idlen = body[20] as usize;
    assert_eq!(idlen, 65, "keyid must be the uncompressed sender key");
    let as_public = &body[21..21 + idlen];
    let ciphertext = &body[21 + idlen..];

    let sender_public = PublicKey::from_sec1_bytes(as_public).unwrap();
    let shared =
        p256::ecdh::diffie_hellman(ua_secret.to_nonzero_scalar(), sender_public.as_affine());

    let mut auth_info = Vec::new();
    auth_info.extend_from_slice(b"WebPush: info\0");
    auth_info.extend_from_slice(ua_public);
    auth_info.extend_from_slice(as_public);
    let mut ikm = [0_u8; 32];
    Hkdf::<Sha256>::new(Some(auth_secret), shared.raw_secret_bytes())
        .expand(&auth_info, &mut ikm)
        .unwrap();

    let hkdf = Hkdf::<Sha256>::new(Some(salt), &ikm);
    let mut cek = [0_u8; 16];
    hkdf.expand(b"Content-Encoding: aes128gcm\0", &mut cek)
        .unwrap();
    let mut nonce = [0_u8; 12];
    hkdf.expand(b"Content-Encoding: nonce\0", &mut nonce)
        .unwrap();

    let cipher = Aes128Gcm::new_from_slice(&cek).unwrap();
    let nonce_ref = aes_gcm::Nonce::from(nonce);
    let mut plaintext = cipher.decrypt(&nonce_ref, ciphertext).unwrap();
    assert_eq!(plaintext.pop(), Some(0x02), "last-record delimiter");
    plaintext
}

#[tokio::test]
async fn the_ciphertext_round_trips_with_the_receiver_key() {
    let ua_secret = random_secret();
    let ua_public = uncompressed(&ua_secret.public_key());
    let mut auth_secret = [0_u8; 16];
    getrandom::fill(&mut auth_secret).unwrap();

    let subs = Arc::new(FakeSubs::default());
    subs.rows.lock().unwrap().push(PushSubscription {
        id: "sub-1".into(),
        user_id: "r1".into(),
        endpoint: "https://fcm.googleapis.com/fcm/send/abc".into(),
        p256dh: b64(&ua_public),
        auth: b64(&auth_secret),
        user_agent: None,
        created_at: 1,
        updated_at: 1,
    });
    let transport = FakeTransport::new(201);
    let notifier = WebPushNotifier::new(&webpush_config(), subs as Arc<dyn PushSubscriptionStore>)
        .with_transport(transport.clone());

    let mut data = HashMap::new();
    data.insert("alert_id".into(), "alert-1".into());
    notifier
        .send_to_user("r1", "SOS Alert", "help", &data)
        .await
        .unwrap();

    let captured = transport.captured.lock().unwrap();
    let plaintext = decrypt(&captured[0].body, &ua_secret, &auth_secret, &ua_public);
    let json: serde_json::Value = serde_json::from_slice(&plaintext).unwrap();
    assert_eq!(json["title"], "SOS Alert");
    assert_eq!(json["body"], "help");
    assert_eq!(json["data"]["alert_id"], "alert-1");
    assert_eq!(json["data"]["url"], "/");
}

#[test]
fn the_vapid_jwt_is_a_verifiable_es256_token() {
    let notifier = WebPushNotifier::new(&webpush_config(), Arc::new(FakeSubs::default()));
    let endpoint = "https://fcm.googleapis.com/fcm/send/abc";
    let token = notifier.vapid_token(endpoint).unwrap();

    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3, "JWT must have three parts");

    let header: serde_json::Value = serde_json::from_slice(&unb64(parts[0])).unwrap();
    assert_eq!(header["alg"], "ES256");
    assert_eq!(header["typ"], "JWT");

    let claims: serde_json::Value = serde_json::from_slice(&unb64(parts[1])).unwrap();
    assert_eq!(claims["aud"], "https://fcm.googleapis.com");
    assert_eq!(claims["sub"], "mailto:sos@example.com");
    let expires = claims["exp"].as_i64().unwrap();
    assert!(expires > 0, "exp must be set");

    // The signature verifies with the configured public key.
    let public = PublicKey::from_sec1_bytes(&unb64(VAPID_PUBLIC)).unwrap();
    let verifying = VerifyingKey::from(public);
    let signature = Signature::from_slice(&unb64(parts[2])).unwrap();
    verifying
        .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)
        .expect("vapid signature must verify");

    // Any change to the signing input must not verify.
    let mut tampered = format!("{}.{}", parts[0], parts[1]);
    tampered.push('A');
    assert!(verifying.verify(tampered.as_bytes(), &signature).is_err());
}

#[test]
fn the_authorization_header_pairs_the_token_with_the_public_key() {
    let notifier = WebPushNotifier::new(&webpush_config(), Arc::new(FakeSubs::default()));
    let header = notifier
        .vapid_authorization("https://fcm.googleapis.com/fcm/send/abc")
        .unwrap();
    assert!(header.starts_with("vapid t="), "{header}");
    assert!(header.contains(", k="), "{header}");
    assert!(header.contains(VAPID_PUBLIC), "{header}");
}
