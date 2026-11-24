//! `/api/push/*` over HTTP against real Postgres.

mod common;

use common::*;

/// Throwaway VAPID keypair (public point + raw private scalar, base64url).
const VAPID_PUBLIC: &str =
    "BF1slwHKJE8AOcOh2SY_LPsCN3UhX0_T8l0q6iu5ylZNRzASaZbmZx2AmP64PmgF3xn627KYvU_mnAhH7TF9K3Y";
const VAPID_PRIVATE: &str = "cev4pqFs3hn-rX0ISjOT0OG2yKAFGF8WW967rv7D7WI";

fn enable_webpush(cfg: &mut sos_allert::config::Config) {
    cfg.webpush.enabled = true;
    cfg.webpush.vapid_public_key = VAPID_PUBLIC.into();
    cfg.webpush.vapid_private_key = VAPID_PRIVATE.into();
}

#[tokio::test]
async fn subscribe_list_resubscribe_and_unsubscribe() {
    let Some(h) = harness_custom(enable_webpush).await else {
        return;
    };
    let owner = unique_id("rs-push");
    let token = signup_and_login(&h.app, &owner, "p").await;

    let (status, body) = send(
        &h.app,
        "GET",
        "/api/push/vapid-public-key",
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["public_key"], VAPID_PUBLIC);

    let endpoint = format!(
        "https://fcm.googleapis.com/fcm/send/{}",
        unique_id("rs-endpoint")
    );
    let (status, body) = send(
        &h.app,
        "POST",
        "/api/push/subscriptions",
        Some(&token),
        Some(serde_json::json!({
            "endpoint": endpoint,
            "p256dh": "BP7d2v9k",
            "auth": "9v2d2v9k",
            "user_agent": "Mozilla/5.0"
        })),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let first_id = body["data"]["id"].as_str().unwrap().to_owned();

    let (status, body) = send(&h.app, "GET", "/api/push/subscriptions", Some(&token), None).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0]["endpoint"], endpoint.as_str());

    // Same browser re-subscribes: refreshed in place, still one row.
    let (status, body) = send(
        &h.app,
        "POST",
        "/api/push/subscriptions",
        Some(&token),
        Some(serde_json::json!({
            "endpoint": endpoint,
            "p256dh": "BNewkey",
            "auth": "newauth"
        })),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["id"], first_id, "upsert must keep the row id");

    let (status, body) = send(&h.app, "GET", "/api/push/subscriptions", Some(&token), None).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().unwrap().len(), 1, "duplicate row");

    // Unsubscribe is idempotent.
    for _ in 0..2 {
        let (status, body) = send(
            &h.app,
            "DELETE",
            "/api/push/subscriptions",
            Some(&token),
            Some(serde_json::json!({ "endpoint": endpoint })),
        )
        .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"], true);
    }

    let (status, body) = send(&h.app, "GET", "/api/push/subscriptions", Some(&token), None).await;
    assert_eq!(status, 200);
    assert!(body["data"].as_array().unwrap().is_empty());

    delete_user(&h.state, &owner).await;
}

#[tokio::test]
async fn an_endpoint_belongs_to_its_first_browser() {
    let Some(h) = harness_custom(enable_webpush).await else {
        return;
    };
    let first = unique_id("rs-push-a");
    let second = unique_id("rs-push-b");
    let first_token = signup_and_login(&h.app, &first, "p").await;
    let second_token = signup_and_login(&h.app, &second, "p").await;

    let endpoint = format!(
        "https://updates.push.services.mozilla.com/wpush/v2/{}",
        unique_id("rs-owned")
    );
    let (status, _) = send(
        &h.app,
        "POST",
        "/api/push/subscriptions",
        Some(&first_token),
        Some(serde_json::json!({"endpoint": endpoint, "p256dh": "k", "auth": "a"})),
    )
    .await;
    assert_eq!(status, 200);

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/push/subscriptions",
        Some(&second_token),
        Some(serde_json::json!({"endpoint": endpoint, "p256dh": "k2", "auth": "a2"})),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert!(body["errors"]
        .as_str()
        .unwrap()
        .contains("already registered"));

    delete_user(&h.state, &first).await;
    delete_user(&h.state, &second).await;
}

#[tokio::test]
async fn a_foreign_or_plain_http_endpoint_is_rejected() {
    let Some(h) = harness_custom(enable_webpush).await else {
        return;
    };
    let user = unique_id("rs-push-bad");
    let token = signup_and_login(&h.app, &user, "p").await;

    for endpoint in [
        "http://fcm.googleapis.com/fcm/send/x",
        "https://169.254.169.254/latest/meta-data",
        "https://evil.example/push",
    ] {
        let (status, body) = send(
            &h.app,
            "POST",
            "/api/push/subscriptions",
            Some(&token),
            Some(serde_json::json!({"endpoint": endpoint, "p256dh": "k", "auth": "a"})),
        )
        .await;
        assert_eq!(status, 400, "{endpoint}: {body}");
    }

    delete_user(&h.state, &user).await;
}

#[tokio::test]
async fn the_vapid_endpoint_is_503_when_web_push_is_off() {
    // Default config: webpush.enabled is false.
    let Some(h) = harness().await else { return };
    let user = unique_id("rs-push-off");
    let token = signup_and_login(&h.app, &user, "p").await;

    let (status, body) = send(
        &h.app,
        "GET",
        "/api/push/vapid-public-key",
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, 503, "{body}");

    delete_user(&h.state, &user).await;
}
