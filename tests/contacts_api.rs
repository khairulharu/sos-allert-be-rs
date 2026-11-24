//! Contacts and addresses over HTTP against real Postgres.

mod common;

use common::*;

#[tokio::test]
async fn contact_crud_is_scoped_to_the_session_user() {
    let Some(h) = harness().await else { return };
    let owner = unique_id("rs-owner");
    let token = signup_and_login(&h.app, &owner, "p").await;
    let stranger = unique_id("rs-stranger");
    let stranger_token = signup_and_login(&h.app, &stranger, "p").await;

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/contacts",
        Some(&token),
        Some(serde_json::json!({
            "first_name": "Budi",
            "last_name": "Sitorus",
            "email": "budi@example.com",
            "phone": "0812"
        })),
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    let contact_id = body["data"]["id"].as_str().unwrap().to_owned();
    assert_eq!(body["data"]["first_name"], "Budi");

    // Listing and paging.
    let (status, body) = send(
        &h.app,
        "GET",
        "/api/contacts?page=1&size=10&name=bud",
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["paging"]["total_item"], 1);
    assert_eq!(body["paging"]["page"], 1);

    // Case-insensitive filter (ILIKE, matching MySQL's collation).
    let (status, body) = send(&h.app, "GET", "/api/contacts?name=BUDI", Some(&token), None).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);

    // Another user cannot read it.
    let (status, _) = send(
        &h.app,
        "GET",
        &format!("/api/contacts/{contact_id}"),
        Some(&stranger_token),
        None,
    )
    .await;
    assert_eq!(status, 404);

    // Update.
    let (status, body) = send(
        &h.app,
        "PUT",
        &format!("/api/contacts/{contact_id}"),
        Some(&token),
        Some(serde_json::json!({"first_name": "Sari", "last_name": "", "email": "sari@example.com", "phone": ""})),
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    assert_eq!(body["data"]["first_name"], "Sari");

    // Parity with the Go validator: `email` runs even when the string is
    // empty (no `omitempty` on that tag), so clearing the email is a 400.
    let (status, _) = send(
        &h.app,
        "PUT",
        &format!("/api/contacts/{contact_id}"),
        Some(&token),
        Some(serde_json::json!({"first_name": "Sari", "email": ""})),
    )
    .await;
    assert_eq!(
        status, 400,
        "empty email must be rejected like Go's validator"
    );

    // Address nested under the contact.
    let (status, body) = send(
        &h.app,
        "POST",
        &format!("/api/contacts/{contact_id}/addresses"),
        Some(&token),
        Some(serde_json::json!({"street": "Jl. Merdeka 1", "city": "Jakarta", "postal_code": "10110"})),
    )
    .await;
    assert_eq!(status, 200, "body: {body}");
    let address_id = body["data"]["id"].as_str().unwrap().to_owned();

    let (status, body) = send(
        &h.app,
        "GET",
        &format!("/api/contacts/{contact_id}/addresses"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);

    // The stranger cannot add an address to somebody else's contact.
    let (status, _) = send(
        &h.app,
        "POST",
        &format!("/api/contacts/{contact_id}/addresses"),
        Some(&stranger_token),
        Some(serde_json::json!({"street": "x"})),
    )
    .await;
    assert_eq!(status, 404);

    let (status, _) = send(
        &h.app,
        "DELETE",
        &format!("/api/contacts/{contact_id}/addresses/{address_id}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, 200);

    let (status, _) = send(
        &h.app,
        "DELETE",
        &format!("/api/contacts/{contact_id}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, 200);

    let (status, _) = send(
        &h.app,
        "GET",
        &format!("/api/contacts/{contact_id}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(status, 404);

    delete_user(&h.state, &owner).await;
    delete_user(&h.state, &stranger).await;
}

#[tokio::test]
async fn contact_validation_and_paging_bounds() {
    let Some(h) = harness().await else { return };
    let owner = unique_id("rs-valid");
    let token = signup_and_login(&h.app, &owner, "p").await;

    let (status, body) = send(
        &h.app,
        "POST",
        "/api/contacts",
        Some(&token),
        Some(serde_json::json!({"first_name": "", "email": "nope"})),
    )
    .await;
    assert_eq!(status, 400, "body: {body}");

    let (status, _) = send(&h.app, "GET", "/api/contacts?size=500", Some(&token), None).await;
    assert_eq!(status, 400, "size above 100 must be rejected");

    let (status, _) = send(&h.app, "GET", "/api/contacts?page=0", Some(&token), None).await;
    assert_eq!(status, 400, "page must be >= 1");

    delete_user(&h.state, &owner).await;
}
