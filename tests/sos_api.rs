//! SOS flow over HTTP against real Postgres and Redis.

mod common;

use common::*;

async fn heartbeat(app: &axum::Router, token: &str, longitude: f64, latitude: f64) {
    let (status, body) = send(
        app,
        "PUT",
        "/api/location",
        Some(token),
        Some(serde_json::json!({"longitude": longitude, "latitude": latitude})),
    )
    .await;
    assert_eq!(status, 200, "heartbeat: {body}");
}

#[tokio::test]
async fn trigger_detail_respond_resolve_history_summary() {
    let Some(h) = harness_custom(|cfg| cfg.sos.grace_period_seconds = 0).await else {
        return;
    };

    let sender = unique_id("rs-sos-sender");
    let responder = unique_id("rs-sos-responder");
    let sender_token = signup_and_login(&h.app, &sender, "p").await;
    let responder_token = signup_and_login(&h.app, &responder, "p").await;
    let (lon, lat) = unique_area();

    heartbeat(&h.app, &responder_token, lon + 0.01, lat + 0.01).await;

    // Trigger by body coordinates, retried with the same idempotency key.
    let (status, body) = send(
        &h.app,
        "POST",
        "/api/sos",
        Some(&sender_token),
        Some(serde_json::json!({"longitude": lon, "latitude": lat})),
    )
    .await;
    // First call has no idempotency header, so just check the shape.
    assert_eq!(status, 201, "trigger: {body}");
    let alert_id = body["data"]["alert_id"].as_str().unwrap().to_owned();
    assert_eq!(body["data"]["status"], "TRIGGERED");
    assert_eq!(body["data"]["recipients_notified"], 1);

    // Active list for the sender.
    let (status, body) = send(&h.app, "GET", "/api/sos/active", Some(&sender_token), None).await;
    assert_eq!(status, 200, "active: {body}");
    assert_eq!(body["data"].as_array().unwrap().len(), 1);

    // Inbox for the responder.
    let (status, body) = send(
        &h.app,
        "GET",
        "/api/sos/inbox",
        Some(&responder_token),
        None,
    )
    .await;
    assert_eq!(status, 200, "inbox: {body}");
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0]["alert_id"], alert_id);

    // A recipient sees masked coordinates until they respond.
    let (status, body) = send(
        &h.app,
        "GET",
        &format!("/api/sos/{alert_id}"),
        Some(&responder_token),
        None,
    )
    .await;
    assert_eq!(status, 200, "detail: {body}");
    assert_eq!(body["data"]["masked"], true);
    assert!(body["data"].get("responders").is_none());

    // A stranger gets 404.
    let stranger = unique_id("rs-sos-stranger");
    let stranger_token = signup_and_login(&h.app, &stranger, "p").await;
    let (status, _) = send(
        &h.app,
        "GET",
        &format!("/api/sos/{alert_id}"),
        Some(&stranger_token),
        None,
    )
    .await;
    assert_eq!(status, 404);

    // Respond with a position ~1.1 km away.
    let (status, body) = send(
        &h.app,
        "POST",
        &format!("/api/sos/{alert_id}/respond"),
        Some(&responder_token),
        Some(serde_json::json!({"responder_latitude": lat + 0.01, "responder_longitude": lon})),
    )
    .await;
    assert_eq!(status, 200, "respond: {body}");
    assert!(body["data"]["eta_seconds"].as_i64().unwrap() >= 60);
    assert!(body["data"]["victim_distance_meters"].as_i64().unwrap() >= 1000);

    // A duplicate answer is a conflict.
    let (status, _) = send(
        &h.app,
        "POST",
        &format!("/api/sos/{alert_id}/respond"),
        Some(&responder_token),
        Some(serde_json::json!({"responder_latitude": lat + 0.01, "responder_longitude": lon})),
    )
    .await;
    assert_eq!(status, 409);

    // Now the responder is a participant: exact coordinates and a roster.
    let (status, body) = send(
        &h.app,
        "GET",
        &format!("/api/sos/{alert_id}"),
        Some(&responder_token),
        None,
    )
    .await;
    assert_eq!(status, 200);
    // `masked` is omitempty in Go, so an unmasked detail omits the key.
    assert!(body["data"].get("masked").is_none());
    assert_eq!(body["data"]["status"], "ACKNOWLEDGED");
    assert_eq!(body["data"]["responders_en_route"], 1);

    // Breadcrumbs: sender only, and the trail is readable.
    let (status, body) = send(
        &h.app,
        "POST",
        &format!("/api/sos/{alert_id}/breadcrumbs"),
        Some(&sender_token),
        Some(serde_json::json!({"latitude": lat + 0.001, "longitude": lon + 0.001})),
    )
    .await;
    assert_eq!(status, 200, "breadcrumb: {body}");
    let (status, body) = send(
        &h.app,
        "GET",
        &format!("/api/sos/{alert_id}/breadcrumbs"),
        Some(&responder_token),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);

    let (status, _) = send(
        &h.app,
        "POST",
        &format!("/api/sos/{alert_id}/breadcrumbs"),
        Some(&responder_token),
        Some(serde_json::json!({"latitude": lat + 0.01, "longitude": lon})),
    )
    .await;
    assert_eq!(status, 403, "only the sender may add breadcrumbs");

    // Resolve, then history and summary.
    let (status, body) = send(
        &h.app,
        "POST",
        &format!("/api/sos/{alert_id}/resolve"),
        Some(&sender_token),
        None,
    )
    .await;
    assert_eq!(status, 200, "resolve: {body}");
    assert_eq!(body["data"]["status"], "RESOLVED");

    let (status, body) = send(
        &h.app,
        "GET",
        "/api/sos/history?role=sent",
        Some(&sender_token),
        None,
    )
    .await;
    assert_eq!(status, 200, "history: {body}");
    assert_eq!(body["data"][0]["status"], "RESOLVED");
    assert_eq!(body["paging"]["total_item"], 1);

    let (status, body) = send(&h.app, "GET", "/api/sos/summary", Some(&sender_token), None).await;
    assert_eq!(status, 200, "summary: {body}");
    assert_eq!(body["data"]["sent"]["total"], 1);
    assert_eq!(body["data"]["sent"]["resolved"], 1);
    assert_eq!(body["data"]["responded"]["total"], 0);

    let (status, body) = send(
        &h.app,
        "GET",
        "/api/sos/summary",
        Some(&responder_token),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"]["responded"]["total"], 1);
    assert_eq!(body["data"]["received"]["total"], 1);

    delete_user(&h.state, &sender).await;
    delete_user(&h.state, &responder).await;
    delete_user(&h.state, &stranger).await;
}

#[tokio::test]
async fn idempotency_key_replays_the_same_alert() {
    let Some(h) = harness_custom(|cfg| cfg.sos.grace_period_seconds = 0).await else {
        return;
    };
    let sender = unique_id("rs-idem");
    let token = signup_and_login(&h.app, &sender, "p").await;
    let (lon, lat) = unique_area();

    let ask = || async {
        send(
            &h.app,
            "POST",
            "/api/sos",
            Some(&token),
            Some(serde_json::json!({"longitude": 106.8, "latitude": -6.2})),
        )
        .await
    };

    let (status, first) = {
        let builder = axum::http::Request::builder()
            .method("POST")
            .uri("/api/sos")
            .header("authorization", token.clone())
            .header("content-type", "application/json")
            .header("x-idempotency-key", "retry-1");
        let request = builder
            .body(axum::body::Body::from(
                serde_json::json!({"longitude": lon, "latitude": lat}).to_string(),
            ))
            .unwrap();
        let response = tower::ServiceExt::oneshot(h.app.clone(), request)
            .await
            .unwrap();
        let status = response.status().as_u16();
        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        (
            status,
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
        )
    };
    assert_eq!(status, 201, "{first}");
    let alert_id = first["data"]["alert_id"].as_str().unwrap().to_owned();

    // Same key: same alert, no duplicate row.
    let (status, second) = {
        let builder = axum::http::Request::builder()
            .method("POST")
            .uri("/api/sos")
            .header("authorization", token.clone())
            .header("content-type", "application/json")
            .header("x-idempotency-key", "retry-1");
        let request = builder
            .body(axum::body::Body::from(
                serde_json::json!({"longitude": lon, "latitude": lat}).to_string(),
            ))
            .unwrap();
        let response = tower::ServiceExt::oneshot(h.app.clone(), request)
            .await
            .unwrap();
        let status = response.status().as_u16();
        let bytes = http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        (
            status,
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
        )
    };
    assert_eq!(status, 201, "{second}");
    assert_eq!(second["data"]["alert_id"], alert_id);

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM alerts WHERE sender_id = $1")
        .bind(&sender)
        .fetch_one(h.state.db())
        .await
        .unwrap();
    assert_eq!(count, 1, "a replayed key created a second alert");

    let _ = ask;
    delete_user(&h.state, &sender).await;
}

#[tokio::test]
async fn trigger_without_location_is_422() {
    let Some(h) = harness_custom(|cfg| cfg.sos.grace_period_seconds = 0).await else {
        return;
    };
    let sender = unique_id("rs-noloc");
    let token = signup_and_login(&h.app, &sender, "p").await;

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/sos",
        Some(&token),
        Some(serde_json::json!({})),
    )
    .await;
    assert_eq!(status, 422, "body: {body}");
    assert!(body["errors"]
        .as_str()
        .unwrap()
        .contains("location is required"));

    delete_user(&h.state, &sender).await;
}

#[tokio::test]
async fn cancel_requires_the_safe_pin() {
    let Some(h) = harness_custom(|cfg| cfg.sos.grace_period_seconds = 0).await else {
        return;
    };
    let sender = unique_id("rs-cancel");
    let token = signup_and_login(&h.app, &sender, "p").await;

    // Set the PIN first.
    let (status, body) = send(
        &h.app,
        "PATCH",
        "/api/users/_current",
        Some(&token),
        Some(serde_json::json!({"safe_pin": "1234"})),
    )
    .await;
    assert_eq!(status, 200, "set pin: {body}");

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/sos",
        Some(&token),
        Some(serde_json::json!({"longitude": 106.8, "latitude": -6.2})),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let alert_id = body["data"]["alert_id"].as_str().unwrap().to_owned();

    // Wrong PIN: 403, and the alert stays TRIGGERED.
    let (status, _) = send(
        &h.app,
        "POST",
        &format!("/api/sos/{alert_id}/cancel"),
        Some(&token),
        Some(serde_json::json!({"pin": "9999", "reason": "FALSE_ALARM"})),
    )
    .await;
    assert_eq!(status, 403);

    let (status, body) = send(
        &h.app,
        "POST",
        &format!("/api/sos/{alert_id}/cancel"),
        Some(&token),
        Some(serde_json::json!({"pin": "1234"})),
    )
    .await;
    assert_eq!(status, 200, "cancel: {body}");
    assert_eq!(body["data"]["status"], "CANCELLED");

    let (status, _) = send(
        &h.app,
        "POST",
        &format!("/api/sos/{alert_id}/cancel"),
        Some(&token),
        Some(serde_json::json!({"pin": "1234"})),
    )
    .await;
    assert_eq!(status, 409);

    delete_user(&h.state, &sender).await;
}

#[tokio::test]
async fn grace_period_defers_then_the_outbox_dispatches() {
    let Some(h) = harness_custom(|cfg| cfg.sos.grace_period_seconds = 10).await else {
        return;
    };
    let sender = unique_id("rs-grace");
    let responder = unique_id("rs-grace-r");
    let sender_token = signup_and_login(&h.app, &sender, "p").await;
    let responder_token = signup_and_login(&h.app, &responder, "p").await;
    let (lon, lat) = unique_area();
    heartbeat(&h.app, &responder_token, lon + 0.001, lat + 0.001).await;

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/sos",
        Some(&sender_token),
        Some(serde_json::json!({"longitude": lon, "latitude": lat})),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(body["data"]["grace_period_seconds"], 10);
    let alert_id = body["data"]["alert_id"].as_str().unwrap().to_owned();

    let due: Option<i64> = sqlx::query_scalar("SELECT dispatch_due_at FROM alerts WHERE id = $1")
        .bind(&alert_id)
        .fetch_one(h.state.db())
        .await
        .unwrap();
    assert!(due.is_some(), "dispatch_due_at was not persisted");

    // Not due yet: the outbox scan finds nothing.
    assert_eq!(h.state.sos().dispatch_due(10).await.unwrap(), 0);

    // Force the due time into the past, then the scan dispatches once.
    sqlx::query("UPDATE alerts SET dispatch_due_at = dispatch_due_at - 60000 WHERE id = $1")
        .bind(&alert_id)
        .execute(h.state.db())
        .await
        .unwrap();
    assert_eq!(h.state.sos().dispatch_due(10).await.unwrap(), 1);

    let dispatched_at: Option<i64> =
        sqlx::query_scalar("SELECT dispatched_at FROM alerts WHERE id = $1")
            .bind(&alert_id)
            .fetch_one(h.state.db())
            .await
            .unwrap();
    assert!(dispatched_at.is_some());

    // A second scan is a no-op: the latch holds.
    assert_eq!(h.state.sos().dispatch_due(10).await.unwrap(), 0);

    delete_user(&h.state, &sender).await;
    delete_user(&h.state, &responder).await;
}

#[tokio::test]
async fn per_user_rate_limit_is_429() {
    let Some(h) = harness_custom(|cfg| {
        cfg.sos.grace_period_seconds = 0;
        cfg.sos.rate_limit_per_hour = 1;
    })
    .await
    else {
        return;
    };
    let sender = unique_id("rs-rate");
    let token = signup_and_login(&h.app, &sender, "p").await;

    let (status, _) = send(
        &h.app,
        "POST",
        "/api/sos",
        Some(&token),
        Some(serde_json::json!({"longitude": 106.8, "latitude": -6.2})),
    )
    .await;
    assert_eq!(status, 201);

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/sos",
        Some(&token),
        Some(serde_json::json!({"longitude": 106.8, "latitude": -6.2})),
    )
    .await;
    assert_eq!(status, 429, "body: {body}");

    delete_user(&h.state, &sender).await;
}

#[tokio::test]
async fn description_triage_is_returned_and_persisted() {
    let Some(h) = harness_custom(|cfg| {
        cfg.sos.grace_period_seconds = 0;
        cfg.ai.enabled = true;
        cfg.ai.provider = "offline".into();
    })
    .await
    else {
        return;
    };
    let sender = unique_id("rs-triage");
    let token = signup_and_login(&h.app, &sender, "p").await;
    let (lon, lat) = unique_area();

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/sos",
        Some(&token),
        Some(serde_json::json!({
            "longitude": lon,
            "latitude": lat,
            "description": "ada orang pingsan di pantai"
        })),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(body["data"]["triage"]["emergency_type"], "MEDICAL");
    assert_eq!(body["data"]["triage"]["severity"], 0.6);

    let alert_id = body["data"]["alert_id"].as_str().unwrap().to_owned();
    let row: (String, Option<f64>, Option<String>) =
        sqlx::query_as("SELECT emergency_type, severity, summary FROM alerts WHERE id = $1")
            .bind(&alert_id)
            .fetch_one(h.state.db())
            .await
            .unwrap();
    assert_eq!(row.0, "MEDICAL");
    assert_eq!(row.1, Some(0.6));
    assert!(row.2.unwrap().contains("pingsan"));

    // A description with no signal leaves no triage and the default type.
    let (status, body) = send(
        &h.app,
        "POST",
        "/api/sos",
        Some(&token),
        Some(serde_json::json!({
            "longitude": lon,
            "latitude": lat,
            "description": "mau beli kopi dulu"
        })),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    assert!(body["data"].get("triage").is_none());
    assert_eq!(body["data"]["status"], "TRIGGERED");

    delete_user(&h.state, &sender).await;
}
