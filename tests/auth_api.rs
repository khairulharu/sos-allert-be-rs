//! End-to-end auth flow against a real Postgres and Redis.
//!
//! Run with:
//! `TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55432/sos_rs \
//!  TEST_REDIS_URL=redis://127.0.0.1:56379 cargo test --test auth_api -- --nocapture`
//!
//! Skipped (passing with a note) when those variables are unset.

mod common;

use common::*;

#[tokio::test]
async fn register_login_current_logout_round_trip() {
    let Some(h) = harness().await else { return };
    let id = unique_id("rs-test");

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/users",
        None,
        Some(serde_json::json!({"id": id, "password": "rahasia", "name": "Budi"})),
    )
    .await;
    assert_eq!(status, 200, "register body: {body}");
    assert_eq!(body["data"]["id"], id);
    assert_eq!(body["data"]["name"], "Budi");
    assert!(
        body["data"].get("token").is_none(),
        "register leaks a token"
    );

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/users/_login",
        None,
        Some(serde_json::json!({"id": id, "password": "rahasia"})),
    )
    .await;
    assert_eq!(status, 200, "login body: {body}");
    let token = body["data"]["token"].as_str().expect("token").to_owned();
    assert!(!token.is_empty());
    assert!(
        body["data"].get("id").is_none(),
        "login returns more than a token"
    );

    let (status, body) = send(&h.app, "GET", "/api/users/_current", Some(&token), None).await;
    assert_eq!(status, 200, "current body: {body}");
    assert_eq!(body["data"]["id"], id);
    assert_eq!(body["data"]["name"], "Budi");

    let (status, body) = send(&h.app, "DELETE", "/api/users", Some(&token), None).await;
    assert_eq!(status, 200, "logout body: {body}");
    assert_eq!(body["data"], true);

    let (status, body) = send(&h.app, "GET", "/api/users/_current", Some(&token), None).await;
    assert_eq!(status, 401, "token still works after logout: {body}");

    delete_user(&h.state, &id).await;
}

#[tokio::test]
async fn duplicate_registration_conflicts() {
    let Some(h) = harness().await else { return };
    let id = unique_id("rs-dup");
    let payload = serde_json::json!({"id": id, "password": "p", "name": "N"});

    let (status, _) = send(&h.app, "POST", "/api/users", None, Some(payload.clone())).await;
    assert_eq!(status, 200);

    let (status, body) = send(&h.app, "POST", "/api/users", None, Some(payload)).await;
    assert_eq!(status, 409, "body: {body}");
    assert!(body["errors"].is_string());

    delete_user(&h.state, &id).await;
}

#[tokio::test]
async fn wrong_password_and_unknown_user_are_both_401() {
    let Some(h) = harness().await else { return };
    let id = unique_id("rs-wrong");
    send(
        &h.app,
        "POST",
        "/api/users",
        None,
        Some(serde_json::json!({"id": id, "password": "benar", "name": "N"})),
    )
    .await;

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/users/_login",
        None,
        Some(serde_json::json!({"id": id, "password": "salah"})),
    )
    .await;
    assert_eq!(status, 401, "body: {body}");

    let (status, _) = send(
        &h.app,
        "POST",
        "/api/users/_login",
        None,
        Some(serde_json::json!({"id": "ghost-xyz", "password": "x"})),
    )
    .await;
    assert_eq!(status, 401);

    delete_user(&h.state, &id).await;
}

#[tokio::test]
async fn missing_token_and_bad_body_shapes() {
    let Some(h) = harness().await else { return };

    let (status, body) = send(&h.app, "GET", "/api/users/_current", None, None).await;
    assert_eq!(status, 401, "body: {body}");

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/users",
        None,
        Some(serde_json::json!({"id": "x"})),
    )
    .await;
    assert_eq!(status, 400, "body: {body}");
    assert!(body["errors"].is_string());

    let (status, _) = send(
        &h.app,
        "POST",
        "/api/users",
        None,
        Some(serde_json::json!({"id": "a", "password": "b", "name": "c"})),
    )
    .await;
    // The id is free unless a previous run used it; a 409 here would mean the
    // body was accepted, which is also fine for this assertion.
    assert!(status == 200 || status == 409);
    delete_user(&h.state, "a").await;
}

#[tokio::test]
async fn update_sets_name_and_new_password() {
    let Some(h) = harness().await else { return };
    let id = unique_id("rs-update");
    send(
        &h.app,
        "POST",
        "/api/users",
        None,
        Some(serde_json::json!({"id": id, "password": "lama", "name": "Lama"})),
    )
    .await;

    let (_, body) = send(
        &h.app,
        "POST",
        "/api/users/_login",
        None,
        Some(serde_json::json!({"id": id, "password": "lama"})),
    )
    .await;
    let token = body["data"]["token"].as_str().unwrap().to_owned();

    let (status, body) = send(
        &h.app,
        "PATCH",
        "/api/users/_current",
        Some(&token),
        Some(serde_json::json!({"name": "Baru", "password": "baru", "safe_pin": "1234"})),
    )
    .await;
    assert_eq!(status, 200, "update body: {body}");
    assert_eq!(body["data"]["name"], "Baru");

    let (status, _) = send(
        &h.app,
        "POST",
        "/api/users/_login",
        None,
        Some(serde_json::json!({"id": id, "password": "lama"})),
    )
    .await;
    assert_eq!(status, 401, "old password still works");

    let (status, _) = send(
        &h.app,
        "POST",
        "/api/users/_login",
        None,
        Some(serde_json::json!({"id": id, "password": "baru"})),
    )
    .await;
    assert_eq!(status, 200, "new password rejected");

    // The PIN is stored as a bcrypt hash, never as the raw digits.
    let row: Option<String> = sqlx::query_scalar("SELECT safe_pin_hash FROM users WHERE id = $1")
        .bind(&id)
        .fetch_optional(h.state.db())
        .await
        .unwrap()
        .flatten();
    let hash = row.expect("pin hash stored");
    assert_ne!(hash, "1234");
    assert!(bcrypt::verify("1234", &hash).unwrap());

    delete_user(&h.state, &id).await;
}

#[tokio::test]
async fn logout_on_one_device_invalidates_the_previous_token() {
    let Some(h) = harness().await else { return };
    let id = unique_id("rs-single");
    send(
        &h.app,
        "POST",
        "/api/users",
        None,
        Some(serde_json::json!({"id": id, "password": "p", "name": "N"})),
    )
    .await;

    let (_, first) = send(
        &h.app,
        "POST",
        "/api/users/_login",
        None,
        Some(serde_json::json!({"id": id, "password": "p"})),
    )
    .await;
    let first_token = first["data"]["token"].as_str().unwrap().to_owned();

    let (_, second) = send(
        &h.app,
        "POST",
        "/api/users/_login",
        None,
        Some(serde_json::json!({"id": id, "password": "p"})),
    )
    .await;
    let second_token = second["data"]["token"].as_str().unwrap().to_owned();

    assert_ne!(first_token, second_token);
    let (status, _) = send(
        &h.app,
        "GET",
        "/api/users/_current",
        Some(&first_token),
        None,
    )
    .await;
    assert_eq!(status, 401, "old session survived a second login");
    let (status, _) = send(
        &h.app,
        "GET",
        "/api/users/_current",
        Some(&second_token),
        None,
    )
    .await;
    assert_eq!(status, 200);

    delete_user(&h.state, &id).await;
}
