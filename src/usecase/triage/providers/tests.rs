//! Provider request shapes and the chain, against a local mock server.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, Uri};
use axum::routing::any;
use axum::Router;
use serde_json::json;

use crate::config::AiProviderConfig;
use crate::usecase::triage::Triage;

use super::{
    parse_jev_answers, parse_triage_json, AiTriage, JevClient, OllamaClient, OpenAiClient,
};

#[derive(Debug, Clone, Default)]
struct Captured {
    method: Method,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Captured>>>);

impl Capture {
    fn take(&self) -> Vec<Captured> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

#[allow(clippy::too_many_arguments)]
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
    capture.0.lock().unwrap().push(Captured {
        method,
        path: uri.path().to_owned(),
        headers: header_map,
        body: body.to_vec(),
    });
    "{}"
}

/// Mock server that always answers with `response_body`.
async fn mock_server(response_body: serde_json::Value) -> (String, Capture) {
    let capture = Capture::default();
    let body = Arc::new(response_body.to_string());
    let app = Router::new()
        .fallback(any(
            move |State(capture): State<Capture>,
                  method: Method,
                  uri: Uri,
                  headers: HeaderMap,
                  request_body: Bytes| {
                let body = body.clone();
                async move {
                    record(State(capture), method, uri, headers, request_body).await;
                    axum::response::Response::builder()
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body.to_string()))
                        .unwrap()
                }
            },
        ))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), capture)
}

fn config(base_url: &str, api_key: &str, model: &str) -> AiProviderConfig {
    AiProviderConfig {
        base_url: base_url.to_string(),
        api_key: api_key.to_string(),
        model: model.to_string(),
    }
}

#[test]
fn triage_json_is_validated_strictly() {
    let ok =
        parse_triage_json(br#"{"emergency_type":"MEDICAL","severity":0.6,"summary":"pingsan"}"#)
            .unwrap();
    assert_eq!(ok.emergency_type, "MEDICAL");
    assert_eq!(ok.severity, 0.6);

    // 0 is a real score.
    let zero = parse_triage_json(br#"{"emergency_type":"DANGER","severity":0}"#).unwrap();
    assert_eq!(zero.severity, 0.0);

    assert!(parse_triage_json(br#"{"emergency_type":"FIRE","severity":0.5}"#).is_err());
    assert!(parse_triage_json(br#"{"emergency_type":"MEDICAL"}"#).is_err());
    assert!(parse_triage_json(br#"{"emergency_type":"MEDICAL","severity":1.4}"#).is_err());
    assert!(parse_triage_json(b"not json").is_err());
}

#[test]
fn jev_answers_are_validated_and_normalised() {
    let payload = json!({
        "answers": {
            "emergency_type": { "type": "choice", "choice": "MEDICAL", "confidence": 0.9 },
            "severity": { "type": "score", "score": 4.0 }
        }
    });
    let result = parse_jev_answers(&payload).unwrap();
    assert_eq!(result.emergency_type, "MEDICAL");
    assert!((result.severity - 1.0).abs() < 1e-9);
    assert!(!result.summary.is_empty());

    // Low confidence is no signal.
    let low = json!({
        "answers": {
            "emergency_type": { "choice": "DANGER", "confidence": 0.2 },
            "severity": { "score": 1.0 }
        }
    });
    assert!(parse_jev_answers(&low).is_err());

    // Out-of-range score and unknown choice are errors.
    let out_of_range = json!({
        "answers": {
            "emergency_type": { "choice": "DANGER", "confidence": 0.9 },
            "severity": { "score": 9.0 }
        }
    });
    assert!(parse_jev_answers(&out_of_range).is_err());

    let unknown = json!({
        "answers": {
            "emergency_type": { "choice": "FIRE", "confidence": 0.9 },
            "severity": { "score": 2.0 }
        }
    });
    assert!(parse_jev_answers(&unknown).is_err());
}

#[tokio::test]
async fn openai_request_shape() {
    let (base, capture) = mock_server(json!({
        "choices": [{ "message": { "content": "{\"emergency_type\":\"ACCIDENT\",\"severity\":0.3,\"summary\":\"kecelakaan\"}" } }]
    }))
    .await;
    let client = OpenAiClient::new(&config(&base, "key-1", "gpt-test"), 5);

    let result = client.triage("kecelakaan motor").await;
    assert_eq!(result.emergency_type, "ACCIDENT");
    assert_eq!(result.severity, 0.3);

    let requests = capture.take();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::POST);
    assert_eq!(requests[0].path, "/chat/completions");
    assert_eq!(
        requests[0].headers.get("authorization").map(String::as_str),
        Some("Bearer key-1")
    );
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["model"], "gpt-test");
    assert_eq!(body["messages"][1]["content"], "kecelakaan motor");
    assert_eq!(body["response_format"]["type"], "json_object");
    assert_eq!(body["temperature"], 0);
}

#[tokio::test]
async fn ollama_request_shape() {
    let (base, capture) = mock_server(json!({
        "message": { "content": "{\"emergency_type\":\"DANGER\",\"severity\":0.8}" },
        "done": true
    }))
    .await;
    let client = OllamaClient::new(&config(&base, "", "llama3"), 5);

    let result = client.triage("diancam pisau").await;
    assert_eq!(result.emergency_type, "DANGER");

    let requests = capture.take();
    assert_eq!(requests[0].path, "/api/chat");
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["stream"], false);
    assert_eq!(body["format"], "json");
}

#[tokio::test]
async fn jev_request_shape() {
    let (base, capture) = mock_server(json!({
        "answers": {
            "emergency_type": { "type": "choice", "choice": "DANGER", "confidence": 0.95 },
            "severity": { "type": "score", "score": 3.0 }
        }
    }))
    .await;
    let client = JevClient::new(&config(&base, "jev-key", "jev-test"), 5);

    let result = client.triage("kebakaran").await;
    assert_eq!(result.emergency_type, "DANGER");
    assert!((result.severity - 0.75).abs() < 1e-9);

    let requests = capture.take();
    assert_eq!(requests[0].path, "/systemone");
    assert_eq!(
        requests[0].headers.get("authorization").map(String::as_str),
        Some("Bearer jev-key")
    );
    let body: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body["model"], "jev-test");
    assert_eq!(body["state"]["language"], "id");
    assert_eq!(body["questions"]["emergency_type"]["type"], "choice");
    assert_eq!(body["questions"]["severity"]["type"], "score");
}

#[tokio::test]
async fn an_empty_description_never_calls_the_provider() {
    let (base, capture) = mock_server(json!({})).await;
    let client = OpenAiClient::new(&config(&base, "key", "model"), 5);
    let result = client.triage("   ").await;
    assert!(!result.is_signal());
    assert!(capture.take().is_empty());
}

#[tokio::test]
async fn a_bad_answer_falls_through_the_chain_to_the_next_provider() {
    // Provider 1 answers garbage; provider 2 answers correctly.
    let (bad_base, _bad) =
        mock_server(json!({"choices": [{ "message": { "content": "no json" } }]})).await;
    let (good_base, _good) = mock_server(json!({
        "choices": [{ "message": { "content": "{\"emergency_type\":\"MEDICAL\",\"severity\":0.9}" } }]
    }))
    .await;

    let chain = AiTriage::new(vec![
        Arc::new(OpenAiClient::new(&config(&bad_base, "k", "m"), 5)),
        Arc::new(OpenAiClient::new(&config(&good_base, "k", "m"), 5)),
    ]);

    let result = chain.triage("pingsan").await;
    assert_eq!(result.emergency_type, "MEDICAL");
    assert_eq!(result.severity, 0.9);
}

#[tokio::test]
async fn a_no_signal_answer_also_falls_through() {
    // Provider 1 returns a valid JSON with no emergency type signal? It must
    // be rejected by the parser, so the chain moves on.
    let (first_base, _) = mock_server(json!({"answers": {}})).await;
    let (second_base, _) = mock_server(json!({
        "answers": {
            "emergency_type": { "choice": "ACCIDENT", "confidence": 0.9 },
            "severity": { "score": 2.0 }
        }
    }))
    .await;

    let chain = AiTriage::new(vec![
        Arc::new(JevClient::new(&config(&first_base, "k", "m"), 5)),
        Arc::new(JevClient::new(&config(&second_base, "k", "m"), 5)),
    ]);
    let result = chain.triage("tabrakan").await;
    assert_eq!(result.emergency_type, "ACCIDENT");
}

#[tokio::test]
async fn an_empty_chain_is_no_signal() {
    let chain = AiTriage::new(Vec::new());
    assert!(chain.is_empty());
    assert!(!chain.triage("pingsan").await.is_signal());
}

/// Keeps the trait object shape used by the bootstrap wiring honest.
#[allow(dead_code)]
fn _engine_type(engine: Arc<dyn Triage>) -> Arc<dyn Triage> {
    engine
}
