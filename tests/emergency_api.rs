//! Emergency contacts over HTTP against real Postgres.

mod common;

use common::*;

#[tokio::test]
async fn create_list_and_reject_bad_numbers() {
    let Some(h) = harness().await else { return };
    let user = unique_id("rs-sos-contact");
    let token = signup_and_login(&h.app, &user, "p").await;

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/emergency-contacts",
        Some(&token),
        Some(serde_json::json!({
            "contact_name": "Ibu",
            "phone_number": "+62 812-3456-7890",
            "relationship": "family"
        })),
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert_eq!(body["data"]["phone_number"], "+6281234567890");
    assert_eq!(body["data"]["is_verified"], false);

    // Duplicate in any formatting.
    let (status, body) = send(
        &h.app,
        "POST",
        "/api/emergency-contacts",
        Some(&token),
        Some(serde_json::json!({
            "contact_name": "Ibu",
            "phone_number": "+62 812 3456 7890",
            "relationship": "family"
        })),
    )
    .await;
    assert_eq!(status, 422, "body: {body}");

    // Not dialable internationally.
    let (status, _) = send(
        &h.app,
        "POST",
        "/api/emergency-contacts",
        Some(&token),
        Some(serde_json::json!({
            "contact_name": "Kode premium",
            "phone_number": "1234",
            "relationship": "other"
        })),
    )
    .await;
    assert_eq!(status, 422);

    let (status, body) = send(&h.app, "GET", "/api/emergency-contacts", Some(&token), None).await;
    assert_eq!(status, 200, "body: {body}");
    let rows = body["data"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["contact_name"], "Ibu");

    // Another user sees nothing.
    let other = unique_id("rs-sos-other");
    let other_token = signup_and_login(&h.app, &other, "p").await;
    let (status, body) = send(
        &h.app,
        "GET",
        "/api/emergency-contacts",
        Some(&other_token),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert!(body["data"].as_array().unwrap().is_empty());

    // Unauthenticated access is rejected.
    let (status, _) = send(&h.app, "GET", "/api/emergency-contacts", None, None).await;
    assert_eq!(status, 401);

    delete_user(&h.state, &user).await;
    delete_user(&h.state, &other).await;
}

#[tokio::test]
async fn list_is_capped_by_the_ten_contact_rule() {
    let Some(h) = harness().await else { return };
    let user = unique_id("rs-cap");
    let token = signup_and_login(&h.app, &user, "p").await;

    for index in 0..10 {
        let (status, body) = send(
            &h.app,
            "POST",
            "/api/emergency-contacts",
            Some(&token),
            Some(serde_json::json!({
                "contact_name": format!("Contact {index}"),
                "phone_number": format!("+62812345678{index:02}"),
                "relationship": "family"
            })),
        )
        .await;
        assert_eq!(status, 200, "contact {index}: {body}");
    }

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/emergency-contacts",
        Some(&token),
        Some(serde_json::json!({
            "contact_name": "Eleventh",
            "phone_number": "+6289999999999",
            "relationship": "family"
        })),
    )
    .await;
    assert_eq!(status, 422, "body: {body}");

    delete_user(&h.state, &user).await;
}
