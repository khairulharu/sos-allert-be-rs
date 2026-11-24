//! Audio classify + media transcription over HTTP, with a mock STT provider.

mod common;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Uri};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use common::*;

#[derive(Clone, Default)]
struct SttState {
    responses: Arc<Mutex<VecDeque<serde_json::Value>>>,
    requests: Arc<Mutex<Vec<serde_json::Value>>>,
}

async fn stt_handler(
    State(state): State<SttState>,
    _uri: Uri,
    _headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let text = String::from_utf8_lossy(&body).to_string();
    state.requests.lock().unwrap().push(serde_json::json!({
        "body_contains_file": text.contains("name=\"file\""),
        "body_contains_model": text.contains("whisper-test"),
        "body_contains_prompt": text.contains("Panggilan darurat"),
    }));

    let response = state
        .responses
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or_else(|| serde_json::json!({"text": ""}));
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        response.to_string(),
    )
}

/// Start a mock `/audio/transcriptions` server; responses are dequeued in order.
async fn stt_server(responses: Vec<serde_json::Value>) -> (String, SttState) {
    let state = SttState {
        responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .fallback(post(stt_handler))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), state)
}

fn speech(text: &str) -> serde_json::Value {
    serde_json::json!({
        "text": text,
        "duration": 3.0,
        "segments": [{ "no_speech_prob": 0.05, "avg_logprob": -0.3, "compression_ratio": 1.1 }]
    })
}

fn multipart_audio(boundary: &str, content_type: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"cry.webm\"\r\nContent-Type: {content_type}\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(payload);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

async fn post_multipart(
    app: &axum::Router,
    uri: &str,
    token: &str,
    boundary: &str,
    body: Vec<u8>,
) -> (u16, serde_json::Value) {
    let request = axum::http::Request::builder()
        .method("POST")
        .uri(uri)
        .header("authorization", token)
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .unwrap();
    let response = tower::ServiceExt::oneshot(app.clone(), request)
        .await
        .unwrap();
    let status = response.status().as_u16();
    let bytes = http_body_util::BodyExt::collect(response.into_body())
        .await
        .unwrap()
        .to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}

#[tokio::test]
async fn classify_transcribes_triages_and_hands_off_to_the_trigger() {
    let (stt_url, stt_state) = stt_server(vec![
        speech("ibu saya pingsan dan tidak sadar"),
        speech("terima kasih telah menonton"),
    ])
    .await;

    let Some(h) = harness_custom(move |cfg| {
        cfg.ai.enabled = true;
        cfg.ai.provider = "offline".into();
        cfg.ai.stt.enabled = true;
        cfg.ai.stt.base_url = stt_url.clone();
        cfg.ai.stt.model = "whisper-test".into();
        cfg.sos.grace_period_seconds = 0;
        cfg.sos.classify_rate_limit_per_hour = 10;
    })
    .await
    else {
        return;
    };

    let user = unique_id("rs-classify");
    let token = signup_and_login(&h.app, &user, "p").await;
    let boundary = "----classify";
    let audio = b"fake-webm-bytes".to_vec();

    let (status, body) = post_multipart(
        &h.app,
        "/api/sos/classify",
        &token,
        boundary,
        multipart_audio(boundary, "audio/webm", &audio),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["transcription_status"], "completed");
    assert_eq!(
        body["data"]["transcript"],
        "ibu saya pingsan dan tidak sadar"
    );
    assert_eq!(body["data"]["emergency_type"], "MEDICAL");
    // "pingsan" plus the urgent "tidak sadar" boosts the base 0.6 to 0.8.
    assert_eq!(body["data"]["severity"], 0.8);

    // The multipart request carried the model and the emergency prompt.
    // Scope the guard: holding a std Mutex across an await would block the
    // mock server task on this single-threaded runtime.
    {
        let requests = stt_state.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["body_contains_file"], true);
        assert_eq!(requests[0]["body_contains_model"], true);
        assert_eq!(requests[0]["body_contains_prompt"], true);
    }

    // The next trigger consumes the server-computed triage: no description
    // in the body, yet MEDICAL with the audio severity.
    let (status, body) = send(
        &h.app,
        "POST",
        "/api/sos",
        Some(&token),
        Some(serde_json::json!({"longitude": 106.8, "latitude": -6.2})),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(body["data"]["triage"]["emergency_type"], "MEDICAL");
    assert_eq!(body["data"]["triage"]["severity"], 0.8);

    // A silent clip is "empty", not an error, and stores no triage.
    let (status, body) = post_multipart(
        &h.app,
        "/api/sos/classify",
        &token,
        boundary,
        multipart_audio(boundary, "audio/webm", &audio),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["transcription_status"], "empty");
    assert!(body["data"].get("emergency_type").is_none());

    // Only audio is accepted.
    let (status, _) = post_multipart(
        &h.app,
        "/api/sos/classify",
        &token,
        boundary,
        multipart_audio(boundary, "text/plain", b"hello"),
    )
    .await;
    assert_eq!(status, 400);

    cleanup_geo(&h.state, &[&user]).await;
    delete_user(&h.state, &user).await;
}

#[tokio::test]
async fn classify_is_rate_limited_per_user() {
    let (stt_url, _) = stt_server(vec![
        speech("pingsan"),
        speech("pingsan"),
        speech("pingsan"),
    ])
    .await;

    let Some(h) = harness_custom(move |cfg| {
        cfg.ai.enabled = true;
        cfg.ai.provider = "offline".into();
        cfg.ai.stt.enabled = true;
        cfg.ai.stt.base_url = stt_url.clone();
        cfg.ai.stt.model = "whisper-test".into();
        cfg.sos.grace_period_seconds = 0;
        cfg.sos.classify_rate_limit_per_hour = 2;
    })
    .await
    else {
        return;
    };

    let user = unique_id("rs-classify-rate");
    let token = signup_and_login(&h.app, &user, "p").await;
    let boundary = "----rate";
    let audio = b"fake".to_vec();

    for attempt in 1..=2 {
        let (status, body) = post_multipart(
            &h.app,
            "/api/sos/classify",
            &token,
            boundary,
            multipart_audio(boundary, "audio/webm", &audio),
        )
        .await;
        assert_eq!(status, 200, "attempt {attempt}: {body}");
    }

    let (status, body) = post_multipart(
        &h.app,
        "/api/sos/classify",
        &token,
        boundary,
        multipart_audio(boundary, "audio/webm", &audio),
    )
    .await;
    assert_eq!(status, 429, "{body}");
    assert!(body["errors"].as_str().unwrap().contains("rate limit"));

    cleanup_geo(&h.state, &[&user]).await;
    delete_user(&h.state, &user).await;
}

#[tokio::test]
async fn media_upload_transcribes_and_updates_the_alert_triage() {
    let (stt_url, _) = stt_server(vec![speech("kecelakaan motor, korban berdarah")]).await;

    let Some(h) = harness_custom(move |cfg| {
        cfg.ai.enabled = true;
        cfg.ai.provider = "offline".into();
        cfg.ai.stt.enabled = true;
        cfg.ai.stt.base_url = stt_url.clone();
        cfg.ai.stt.model = "whisper-test".into();
        cfg.sos.grace_period_seconds = 0;
        cfg.storage.local_dir = format!("/tmp/opencode/media-stt-{}", uuid::Uuid::new_v4());
    })
    .await
    else {
        return;
    };

    let user = unique_id("rs-media-stt");
    let token = signup_and_login(&h.app, &user, "p").await;
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

    let boundary = "----media-stt";
    let (status, body) = post_multipart(
        &h.app,
        &format!("/api/sos/{alert_id}/media"),
        &token,
        boundary,
        multipart_audio(boundary, "audio/webm", b"fake-audio"),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["transcription_status"], "completed");
    assert_eq!(body["data"]["triage"]["emergency_type"], "ACCIDENT");
    assert_eq!(
        body["data"]["transcript"],
        "kecelakaan motor, korban berdarah"
    );

    // The alert row now carries the audio-derived classification.
    let row: (String, Option<f64>, Option<String>) =
        sqlx::query_as("SELECT emergency_type, severity, summary FROM alerts WHERE id = $1")
            .bind(&alert_id)
            .fetch_one(h.state.db())
            .await
            .unwrap();
    assert_eq!(row.0, "ACCIDENT");
    assert_eq!(row.1, Some(0.6));

    cleanup_geo(&h.state, &[&user]).await;
    delete_user(&h.state, &user).await;
}
