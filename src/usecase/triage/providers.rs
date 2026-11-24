//! Remote triage providers and the provider chain
//! (`ai_triage.go`, `ai_jev.go`, `ai_openai.go`, `ai_ollama.go`).
//!
//! Every provider is fail-open at the chain level: an error or a
//! no-signal answer moves on to the next classifier, and the offline
//! keyword classifier is normally last. One provider may not block an SOS.

use std::sync::Arc;

use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;

use crate::config::AiProviderConfig;
use crate::usecase::triage::{Triage, TriageResult};

/// Strict, tiny JSON. "format": "json" on Ollama and
/// `response_format: json_object` on OpenAI both rely on this prompt.
pub const TRIAGE_SYSTEM_PROMPT: &str = "You are the emergency triage classifier of an SOS safety app operating in Bali, Indonesia.
Classify a short user distress message and respond ONLY with a JSON object of the form:
{\"emergency_type\": \"DANGER|MEDICAL|ACCIDENT\", \"severity\": <number 0.0 to 1.0>, \"summary\": \"<one short sentence>\"}
DANGER = immediate threat or violence, MEDICAL = health emergency, ACCIDENT = crash/fall/vehicle incident.
The summary must be in Indonesian, max 15 words. Do not output anything but the JSON object.";

/// Below this the Choice answer counts as no signal (TypeSafe's advice is to
/// fall back to another system rather than act on a coin flip).
pub const JEV_MIN_CONFIDENCE: f64 = 0.5;
pub const JEV_QUESTION_TYPE: &str = "emergency_type";
pub const JEV_QUESTION_SEVERITY: &str = "severity";

/// Five ordered levels; the Score answer is the weighted position 0..4.
pub const JEV_SEVERITY_LEVELS: &[&str] = &[
    "No emergency: informational message, no danger",
    "Minor: uncomfortable or needs help, but not life-threatening right now",
    "Moderate: needs a fast response; the condition may get worse",
    "High: life-threatening, needs help immediately",
    "Critical: imminent death, not breathing or cardiac arrest, active violence, many victims",
];

/// A provider's JSON answer, validated strictly: an unknown type, a missing
/// score or an out-of-range score is an error, so the chain falls back.
pub fn parse_triage_json(data: &[u8]) -> Result<TriageResult, String> {
    #[derive(serde::Deserialize)]
    struct Payload {
        emergency_type: String,
        // 0 is a legitimate score, so absence must be distinguishable.
        severity: Option<f64>,
        #[serde(default)]
        summary: String,
    }

    let payload: Payload =
        serde_json::from_slice(data).map_err(|e| format!("invalid provider JSON: {e}"))?;

    if !matches!(
        payload.emergency_type.as_str(),
        "DANGER" | "MEDICAL" | "ACCIDENT"
    ) {
        return Err(format!(
            "unknown emergency_type {:?}",
            payload.emergency_type
        ));
    }
    let Some(severity) = payload.severity else {
        return Err(format!(
            "{} returned no severity score",
            payload.emergency_type
        ));
    };
    if !(0.0..=1.0).contains(&severity) {
        return Err(format!("severity {severity} out of range"));
    }

    Ok(TriageResult {
        emergency_type: payload.emergency_type,
        severity,
        summary: payload.summary.trim().to_string(),
    })
}

fn http_client(timeout_secs: u64) -> Client {
    Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs.max(1)))
        .build()
        .unwrap_or_default()
}

// --------------------------------------------------------------------- OpenAI

/// OpenAI-compatible `/chat/completions` with `json_object` output.
pub struct OpenAiClient {
    base_url: String,
    api_key: String,
    model: String,
    client: Client,
}

impl OpenAiClient {
    pub fn new(config: &AiProviderConfig, timeout_secs: u64) -> Self {
        Self {
            base_url: config.base_url.trim_end_matches('/').to_string(),
            api_key: config.api_key.clone(),
            model: config.model.clone(),
            client: http_client(timeout_secs),
        }
    }

    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }
}

#[async_trait]
impl Triage for OpenAiClient {
    async fn triage(&self, description: &str) -> TriageResult {
        if description.trim().is_empty() {
            return TriageResult::default();
        }
        if self.api_key.is_empty() || self.base_url.is_empty() {
            tracing::warn!("openai triage skipped: not configured");
            return TriageResult::default();
        }

        let body = json!({
            "model": self.model,
            "messages": [
                { "role": "system", "content": TRIAGE_SYSTEM_PROMPT },
                { "role": "user", "content": description }
            ],
            "temperature": 0,
            "response_format": { "type": "json_object" }
        });

        let response = match self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                tracing::warn!(error = %e, "openai triage request failed");
                return TriageResult::default();
            }
        };

        if response.status() != reqwest::StatusCode::OK {
            tracing::warn!(
                status = response.status().as_u16(),
                "openai triage bad status"
            );
            return TriageResult::default();
        }

        let chat: serde_json::Value = match response.json().await {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!(error = %e, "openai triage decode failed");
                return TriageResult::default();
            }
        };

        let content = chat
            .get("choices")
            .and_then(|choices| choices.get(0))
            .and_then(|choice| choice.get("message"))
            .and_then(|message| message.get("content"))
            .and_then(|content| content.as_str())
            .unwrap_or_default()
            .trim()
            .to_string();

        if content.is_empty() {
            tracing::warn!("openai triage returned no content");
            return TriageResult::default();
        }

        match parse_triage_json(content.as_bytes()) {
            Ok(result) => result,
            Err(e) => {
                tracing::warn!(error = %e, "openai triage answer rejected");
                TriageResult::default()
            }
        }
    }
}

// ---------------------------------------------------------------------- Ollama

/// Local Ollama `/api/chat` with structured JSON output.
pub struct OllamaClient {
    base_url: String,
    model: String,
    client: Client,
}

impl OllamaClient {
    pub fn new(config: &AiProviderConfig, timeout_secs: u64) -> Self {
        Self {
            base_url: config.base_url.trim_end_matches('/').to_string(),
            model: config.model.clone(),
            client: http_client(timeout_secs),
        }
    }

    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }
}

#[async_trait]
impl Triage for OllamaClient {
    async fn triage(&self, description: &str) -> TriageResult {
        if description.trim().is_empty() {
            return TriageResult::default();
        }
        if self.base_url.is_empty() {
            return TriageResult::default();
        }

        let body = json!({
            "model": self.model,
            "messages": [
                { "role": "system", "content": TRIAGE_SYSTEM_PROMPT },
                { "role": "user", "content": description }
            ],
            "stream": false,
            "format": "json"
        });

        let response = match self
            .client
            .post(format!("{}/api/chat", self.base_url))
            .json(&body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                tracing::warn!(error = %e, "ollama triage request failed");
                return TriageResult::default();
            }
        };
        if response.status() != reqwest::StatusCode::OK {
            tracing::warn!(
                status = response.status().as_u16(),
                "ollama triage bad status"
            );
            return TriageResult::default();
        }

        let chat: serde_json::Value = match response.json().await {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!(error = %e, "ollama triage decode failed");
                return TriageResult::default();
            }
        };
        let content = chat
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(|content| content.as_str())
            .unwrap_or_default();

        match parse_triage_json(content.as_bytes()) {
            Ok(result) => result,
            Err(e) => {
                tracing::warn!(error = %e, "ollama triage answer rejected");
                TriageResult::default()
            }
        }
    }
}

// ------------------------------------------------------------------------- Jev

/// TypeSafe Jev: one `/v1/systemone` request with two typed questions.
/// Jev does not generate text, so the Indonesian summary is derived locally.
pub struct JevClient {
    base_url: String,
    api_key: String,
    model: String,
    client: Client,
}

impl JevClient {
    pub fn new(config: &AiProviderConfig, timeout_secs: u64) -> Self {
        Self {
            base_url: config.base_url.trim_end_matches('/').to_string(),
            api_key: config.api_key.clone(),
            model: config.model.clone(),
            client: http_client(timeout_secs),
        }
    }

    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }
}

#[async_trait]
impl Triage for JevClient {
    async fn triage(&self, description: &str) -> TriageResult {
        if description.trim().is_empty() {
            return TriageResult::default();
        }
        if self.api_key.is_empty() || self.base_url.is_empty() {
            tracing::warn!("jev triage skipped: not configured");
            return TriageResult::default();
        }

        let choices = json!({
            "DANGER": "Immediate threat to safety: crime (mugging, robbery, kidnapping), violence or assault, being chased, sexual violence, fire, natural disasters (earthquake, flood, landslide), dangerous animals, or a weapon.",
            "MEDICAL": "Medical emergency: unconscious or unresponsive, trouble or stopped breathing, chest pain or cardiac arrest, seizure, heavy bleeding, poisoning, drowning, pregnancy or miscarriage, severe injury, or a severe allergic reaction.",
            "ACCIDENT": "Accident: vehicle collision, fall from height, vehicle went off the road, crushed or trapped, overturned vehicle, brake failure, or a workplace accident."
        });

        let body = json!({
            "model": self.model,
            "state": { "message": description, "language": "id" },
            "questions": {
                JEV_QUESTION_TYPE: {
                    "type": "choice",
                    "instructions": "Which emergency category best fits this SOS message?",
                    "criteria": choices
                },
                JEV_QUESTION_SEVERITY: {
                    "type": "score",
                    "instructions": "How life-threatening and time-critical is this situation right now?",
                    "criteria": JEV_SEVERITY_LEVELS
                }
            }
        });

        let response = match self
            .client
            .post(format!("{}/systemone", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                tracing::warn!(error = %e, "jev triage request failed");
                return TriageResult::default();
            }
        };
        if response.status() != reqwest::StatusCode::OK {
            tracing::warn!(status = response.status().as_u16(), "jev triage bad status");
            return TriageResult::default();
        }

        let payload: serde_json::Value = match response.json().await {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!(error = %e, "jev triage decode failed");
                return TriageResult::default();
            }
        };

        match parse_jev_answers(&payload) {
            Ok(result) => result,
            Err(e) => {
                tracing::warn!(error = %e, "jev triage answer rejected");
                TriageResult::default()
            }
        }
    }
}

/// Validate the two typed answers: known Choice, confidence above the floor,
/// Score inside `0..len-1`.
fn parse_jev_answers(payload: &serde_json::Value) -> Result<TriageResult, String> {
    let answers = payload
        .get("answers")
        .and_then(|value| value.as_object())
        .ok_or("missing answers")?;

    let type_answer = answers
        .get(JEV_QUESTION_TYPE)
        .ok_or_else(|| format!("missing {JEV_QUESTION_TYPE:?} answer"))?;
    let choice = type_answer
        .get("choice")
        .and_then(|value| value.as_str())
        .ok_or("missing choice")?;
    if !matches!(choice, "DANGER" | "MEDICAL" | "ACCIDENT") {
        return Err(format!("unknown emergency_type {choice:?}"));
    }
    let confidence = type_answer
        .get("confidence")
        .and_then(|value| value.as_f64())
        .unwrap_or(0.0);
    if confidence < JEV_MIN_CONFIDENCE {
        return Err(format!("low confidence {confidence:.2}"));
    }

    let severity_answer = answers
        .get(JEV_QUESTION_SEVERITY)
        .ok_or_else(|| format!("missing {JEV_QUESTION_SEVERITY:?} answer"))?;
    let score = severity_answer
        .get("score")
        .and_then(|value| value.as_f64())
        .ok_or("missing score")?;
    let max_score = (JEV_SEVERITY_LEVELS.len() - 1) as f64;
    if score < 0.0 || score > max_score {
        return Err(format!(
            "severity score {score} out of range 0..{max_score}"
        ));
    }

    let severity = score / max_score;
    Ok(TriageResult {
        emergency_type: choice.to_string(),
        severity,
        summary: crate::usecase::triage::summarize(choice, severity, ""),
    })
}

// ----------------------------------------------------------------------- Chain

/// Ordered classifiers: first usable answer wins, errors and no-signal
/// answers fall through. An empty chain yields no signal (the trigger then
/// keeps its request defaults).
pub struct AiTriage {
    chain: Vec<Arc<dyn Triage>>,
}

impl AiTriage {
    pub fn new(engines: Vec<Arc<dyn Triage>>) -> Self {
        Self { chain: engines }
    }

    pub fn len(&self) -> usize {
        self.chain.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chain.is_empty()
    }
}

#[async_trait]
impl Triage for AiTriage {
    async fn triage(&self, description: &str) -> TriageResult {
        for (index, engine) in self.chain.iter().enumerate() {
            let result = engine.triage(description).await;
            if result.is_signal() {
                return result;
            }
            tracing::debug!(
                stage = index + 1,
                total = self.chain.len(),
                "triage stage returned no signal, trying the next classifier"
            );
        }
        TriageResult::default()
    }
}

#[cfg(test)]
mod tests;
