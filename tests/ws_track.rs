//! WebSocket tracking end to end: in-band auth, live broadcasts, budgets.

mod common;

use std::time::Duration;

use common::*;
use futures::{SinkExt, StreamExt};
use sos_allert::delivery::http::routes::router;
use tokio_tungstenite::tungstenite::Message;

/// Serve the router on an ephemeral port; returns the base ws/http address.
async fn serve(state: sos_allert::bootstrap::AppState) -> std::net::SocketAddr {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });
    addr
}

#[tokio::test]
async fn auth_in_band_then_location_updates_arrive() {
    let Some(h) = harness_custom(|cfg| cfg.sos.grace_period_seconds = 0).await else {
        return;
    };
    let sender = unique_id("rs-ws");
    let token = signup_and_login(&h.app, &sender, "p").await;
    let (lon, lat) = unique_area();

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/sos",
        Some(&token),
        Some(serde_json::json!({"longitude": lon, "latitude": lat})),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let alert_id = body["data"]["alert_id"].as_str().unwrap().to_owned();

    let addr = serve(h.state.clone()).await;
    let url = format!("ws://{addr}/ws/sos/{alert_id}/track");
    let (mut socket, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    socket
        .send(Message::Text(
            serde_json::json!({"type": "auth", "token": token})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();

    let first = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("auth_ok timed out")
        .unwrap()
        .unwrap();
    let Message::Text(text) = first else {
        panic!("unexpected frame: {first:?}");
    };
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["type"], "auth_ok");

    // A REST breadcrumb must be broadcast to the open socket.
    let (status, _) = send(
        &h.app,
        "POST",
        &format!("/api/sos/{alert_id}/breadcrumbs"),
        Some(&token),
        Some(serde_json::json!({"latitude": lat + 0.01, "longitude": lon + 0.01})),
    )
    .await;
    assert_eq!(status, 200);

    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("no broadcast")
        .unwrap()
        .unwrap();
    let Message::Text(text) = frame else {
        panic!("unexpected frame: {frame:?}");
    };
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["type"], "LOCATION_UPDATE");
    assert_eq!(value["alert_id"], alert_id);

    let _ = socket.close(None).await;
    cleanup_geo(&h.state, &[&sender]).await;
    delete_user(&h.state, &sender).await;
}

#[tokio::test]
async fn unknown_token_and_non_participants_are_rejected() {
    let Some(h) = harness_custom(|cfg| cfg.sos.grace_period_seconds = 0).await else {
        return;
    };
    let sender = unique_id("rs-ws-sender");
    let stranger = unique_id("rs-ws-stranger");
    let token = signup_and_login(&h.app, &sender, "p").await;
    let stranger_token = signup_and_login(&h.app, &stranger, "p").await;
    let (lon, lat) = unique_area();

    let (_, body) = send(
        &h.app,
        "POST",
        "/api/sos",
        Some(&token),
        Some(serde_json::json!({"longitude": lon, "latitude": lat})),
    )
    .await;
    let alert_id = body["data"]["alert_id"].as_str().unwrap().to_owned();

    let addr = serve(h.state.clone()).await;

    for bad_token in ["not-a-session".to_string(), stranger_token] {
        let url = format!("ws://{addr}/ws/sos/{alert_id}/track");
        let (mut socket, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        socket
            .send(Message::Text(
                serde_json::json!({"type": "auth", "token": bad_token})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();

        let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("no rejection frame")
            .unwrap()
            .unwrap();
        let Message::Text(text) = frame else {
            panic!("unexpected frame: {frame:?}");
        };
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert!(value.get("error").is_some(), "got {value}");
    }

    delete_user(&h.state, &sender).await;
    delete_user(&h.state, &stranger).await;
}

#[tokio::test]
async fn per_user_socket_budget_is_enforced() {
    let Some(h) = harness_custom(|cfg| {
        cfg.sos.grace_period_seconds = 0;
        cfg.sos.ws_max_per_user = 1;
    })
    .await
    else {
        return;
    };
    let sender = unique_id("rs-ws-budget");
    let token = signup_and_login(&h.app, &sender, "p").await;
    let (lon, lat) = unique_area();

    let (_, body) = send(
        &h.app,
        "POST",
        "/api/sos",
        Some(&token),
        Some(serde_json::json!({"longitude": lon, "latitude": lat})),
    )
    .await;
    let alert_id = body["data"]["alert_id"].as_str().unwrap().to_owned();
    let addr = serve(h.state.clone()).await;
    let url = format!("ws://{addr}/ws/sos/{alert_id}/track");

    // First socket authenticates and holds the only user slot.
    let (mut first, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    first
        .send(Message::Text(
            serde_json::json!({"type": "auth", "token": token})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), first.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(frame.to_text().unwrap().contains("auth_ok"));

    // Second socket is rejected after the in-band auth.
    let (mut second, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    second
        .send(Message::Text(
            serde_json::json!({"type": "auth", "token": token})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), second.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let text = frame.to_text().unwrap();
    assert!(text.contains("too many tracking connections"), "got {text}");

    let _ = first.close(None).await;
    cleanup_geo(&h.state, &[&sender]).await;
    delete_user(&h.state, &sender).await;
}
