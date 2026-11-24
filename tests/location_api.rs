//! Location heartbeat, status and nearby over HTTP against real Redis and
//! Postgres.

mod common;

use common::*;

#[tokio::test]
async fn heartbeat_then_status_and_nearby() {
    let Some(h) = harness().await else { return };
    let me = unique_id("rs-me");
    let other = unique_id("rs-other");
    let my_token = signup_and_login(&h.app, &me, "p").await;
    let other_token = signup_and_login(&h.app, &other, "p").await;

    // No heartbeat yet.
    let (status, _) = send(&h.app, "GET", "/api/location", Some(&my_token), None).await;
    assert_eq!(status, 404);

    let (status, body) = send(
        &h.app,
        "PUT",
        "/api/location",
        Some(&my_token),
        Some(serde_json::json!({"longitude": 106.8, "latitude": -6.2})),
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert_eq!(body["data"], true);

    let (status, body) = send(&h.app, "GET", "/api/location", Some(&my_token), None).await;
    assert_eq!(status, 200, "body: {body}");
    assert_eq!(body["data"]["user_id"], me);
    // Redis GEO stores a 52-bit geohash per axis, so a coordinate comes back
    // within ~1e-5 degrees, not bit-identical.
    let latitude = body["data"]["latitude"].as_f64().unwrap();
    assert!((latitude + 6.2).abs() < 1e-4, "latitude {latitude}");

    // The other user is ~700 m away; the history row must exist too.
    send(
        &h.app,
        "PUT",
        "/api/location",
        Some(&other_token),
        Some(serde_json::json!({"longitude": 106.8063, "latitude": -6.2028})),
    )
    .await;

    let (status, body) = send(
        &h.app,
        "GET",
        "/api/location/nearby?radius_m=2000",
        Some(&my_token),
        None,
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert_eq!(body["data"]["masked"], true);
    let responders = body["data"]["responders"].as_array().unwrap();
    let found = responders
        .iter()
        .find(|r| r["user_id"] == other)
        .expect("other user missing from nearby");
    // Masked to 3 decimals and distance snapped to a 100 m bucket.
    let masked_lat = found["latitude"].as_f64().unwrap();
    let masked_lon = found["longitude"].as_f64().unwrap();
    assert!((masked_lat + 6.203).abs() < 1e-4, "masked lat {masked_lat}");
    assert!(
        (masked_lon - 106.806).abs() < 1e-4,
        "masked lon {masked_lon}"
    );
    // Masking keeps 3 decimals only.
    assert_eq!((masked_lat * 1000.0).round() / 1000.0, masked_lat);
    assert_eq!(found["distance_m"].as_i64().unwrap() % 100, 0);
    assert!(!responders.iter().any(|r| r["user_id"] == me));

    // Postgres keeps exactly one history row per user.
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM locations WHERE user_id = $1")
        .bind(&me)
        .fetch_one(h.state.db())
        .await
        .unwrap();
    assert_eq!(rows, 1);

    // A second heartbeat updates the same row.
    send(
        &h.app,
        "PUT",
        "/api/location",
        Some(&my_token),
        Some(serde_json::json!({"longitude": 106.9, "latitude": -6.3})),
    )
    .await;
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM locations WHERE user_id = $1")
        .bind(&me)
        .fetch_one(h.state.db())
        .await
        .unwrap();
    assert_eq!(rows, 1);

    cleanup_geo(&h.state, &[&me, &other]).await;
    delete_user(&h.state, &me).await;
    delete_user(&h.state, &other).await;
}

#[tokio::test]
async fn location_validation_and_radius_bounds() {
    let Some(h) = harness().await else { return };
    let me = unique_id("rs-locval");
    let token = signup_and_login(&h.app, &me, "p").await;

    let (status, _) = send(
        &h.app,
        "PUT",
        "/api/location",
        Some(&token),
        Some(serde_json::json!({"longitude": 106.8})),
    )
    .await;
    assert_eq!(status, 400, "latitude is required");

    let (status, _) = send(
        &h.app,
        "PUT",
        "/api/location",
        Some(&token),
        Some(serde_json::json!({"longitude": 200.0, "latitude": -6.2})),
    )
    .await;
    assert_eq!(status, 400, "longitude is out of range");

    send(
        &h.app,
        "PUT",
        "/api/location",
        Some(&token),
        Some(serde_json::json!({"longitude": 106.8, "latitude": -6.2})),
    )
    .await;

    let (status, _) = send(
        &h.app,
        "GET",
        "/api/location/nearby?radius_m=50",
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, 400);

    let (status, _) = send(
        &h.app,
        "GET",
        "/api/location/nearby?radius_m=9999",
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, 400);

    delete_user(&h.state, &me).await;
}

#[tokio::test]
async fn stale_heartbeat_is_dropped_from_nearby() {
    let Some(h) = harness().await else { return };
    let me = unique_id("rs-stale-me");
    let other = unique_id("rs-stale-other");
    let my_token = signup_and_login(&h.app, &me, "p").await;
    let other_token = signup_and_login(&h.app, &other, "p").await;

    for (token, longitude, latitude) in [(&my_token, 106.8, -6.2), (&other_token, 106.801, -6.201)]
    {
        send(
            &h.app,
            "PUT",
            "/api/location",
            Some(token),
            Some(serde_json::json!({"longitude": longitude, "latitude": latitude})),
        )
        .await;
    }

    // Age the other user's last_seen past the 2 minute cutoff.
    let mut redis = h
        .state
        .redis_client()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let old = 1_i64;
    let _: i64 = redis::cmd("HSET")
        .arg("users:last_seen")
        .arg(&other)
        .arg(old)
        .query_async(&mut redis)
        .await
        .unwrap();

    let (status, body) = send(&h.app, "GET", "/api/location/nearby", Some(&my_token), None).await;
    assert_eq!(status, 200, "body: {body}");
    assert!(
        body["data"]["responders"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["user_id"] != other),
        "stale user still listed"
    );

    // And GET /api/location reports itself stale.
    let (_, body) = send(&h.app, "GET", "/api/location", Some(&other_token), None).await;
    assert_eq!(body["data"]["stale"], true);

    cleanup_geo(&h.state, &[&me, &other]).await;
    delete_user(&h.state, &me).await;
    delete_user(&h.state, &other).await;
}
