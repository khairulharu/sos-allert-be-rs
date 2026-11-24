//! Speech-to-text (`whisper_transcriber.go`, `stt.go`).
//!
//! OpenAI-compatible `/audio/transcriptions`, so Groq, OpenAI and a local
//! faster-whisper server all work. Silence is not a transcript: Whisper
//! happily transcribes room noise into YouTube subtitle boilerplate, so a
//! non-empty `text` is not evidence of speech (#54).

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;

use crate::config::SttConfig;
use crate::error::{AppError, AppResult};

pub const NO_SPEECH_THRESHOLD: f64 = 0.6;
pub const LOW_LOGPROB: f64 = -1.0;
pub const REPEAT_RATIO: f64 = 2.4;
pub const MIN_SPEECH_SECONDS: f64 = 0.5;
pub const SHORT_TEXT_RUNES: usize = 120;

/// Biases the decoder towards an emergency call instead of subtitle
/// boilerplate. Deliberately a short vocabulary list: a full sentence prompt
/// itself leaks into the output on silence.
pub const EMERGENCY_PROMPT: &str = "Panggilan darurat. Tolong, kecelakaan, jatuh, pingsan, \
tidak sadar, sesak napas, berdarah, rumah sakit, ambulans, maling, begal, kebakaran, banjir.";

/// What a transcription produced.
#[derive(Debug, Clone, PartialEq)]
pub enum Transcript {
    /// Real speech.
    Text(String),
    /// The clip held no speech (silence, room noise, hallucinated subtitle).
    Empty,
}

#[async_trait]
pub trait Transcriber: Send + Sync {
    /// `Err` means the provider failed; `Ok(Transcript::Empty)` is a normal
    /// outcome and must not be reported as an error.
    async fn transcribe(
        &self,
        filename: &str,
        content_type: &str,
        audio: &[u8],
    ) -> AppResult<Transcript>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopTranscriber;

#[async_trait]
impl Transcriber for NoopTranscriber {
    async fn transcribe(
        &self,
        _filename: &str,
        _content_type: &str,
        _audio: &[u8],
    ) -> AppResult<Transcript> {
        Err(AppError::Precondition(
            "audio transcription is not configured".into(),
        ))
    }
}

#[derive(Debug, Deserialize)]
pub struct Segment {
    #[serde(default)]
    no_speech_prob: f64,
    #[serde(default)]
    avg_logprob: f64,
    #[serde(default)]
    compression_ratio: f64,
}

#[derive(Debug, Deserialize)]
pub struct WhisperResponse {
    #[serde(default)]
    text: String,
    #[serde(default)]
    duration: f64,
    #[serde(default)]
    segments: Vec<Segment>,
}

pub struct WhisperTranscriber {
    base_url: String,
    api_key: String,
    model: String,
    language: String,
    client: reqwest::Client,
}

impl WhisperTranscriber {
    pub fn new(config: &SttConfig) -> Self {
        let timeout = std::time::Duration::from_secs(config.timeout_seconds.max(1));
        Self {
            base_url: config.base_url.trim_end_matches('/').to_string(),
            api_key: config.api_key.clone(),
            model: config.model.clone(),
            language: config.language.clone(),
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .unwrap_or_default(),
        }
    }

    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }

    async fn transcribe_once(
        &self,
        filename: &str,
        content_type: &str,
        audio: Vec<u8>,
        response_format: &str,
    ) -> Result<WhisperResponse, (Option<u16>, String)> {
        let part = reqwest::multipart::Part::bytes(audio)
            .file_name(filename.to_string())
            .mime_str(content_type)
            .map_err(|e| (None, format!("stt multipart: {e}")))?;

        let form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("model", self.model.clone())
            .text("language", self.language.clone())
            .text("response_format", response_format.to_string())
            .text("prompt", EMERGENCY_PROMPT);

        let mut request = self
            .client
            .post(format!("{}/audio/transcriptions", self.base_url))
            .multipart(form);
        if !self.api_key.is_empty() {
            request = request.bearer_auth(&self.api_key);
        }

        let response = request
            .send()
            .await
            .map_err(|e| (None, format!("stt request: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            let detail: String = response.text().await.unwrap_or_default();
            let detail: String = detail.chars().take(4096).collect();
            return Err((
                Some(status.as_u16()),
                format!("stt status {}: {}", status.as_u16(), detail.trim()),
            ));
        }

        response
            .json::<WhisperResponse>()
            .await
            .map_err(|e| (None, format!("stt decode: {e}")))
    }
}

#[async_trait]
impl Transcriber for WhisperTranscriber {
    async fn transcribe(
        &self,
        filename: &str,
        content_type: &str,
        audio: &[u8],
    ) -> AppResult<Transcript> {
        if self.base_url.is_empty() || self.model.is_empty() {
            return Err(AppError::Precondition("stt is not configured".into()));
        }
        if audio.is_empty() {
            return Ok(Transcript::Empty);
        }
        let filename = if filename.trim().is_empty() {
            "audio.webm"
        } else {
            filename
        };

        let payload = match self
            .transcribe_once(filename, content_type, audio.to_vec(), "verbose_json")
            .await
        {
            Ok(payload) => payload,
            // Not every OpenAI-compatible server accepts verbose_json; the
            // plain shape still yields a transcript, only without the
            // silence scores, so retry once on an explicit 400.
            Err((Some(400), _)) => self
                .transcribe_once(filename, content_type, audio.to_vec(), "json")
                .await
                .map_err(|(_, message)| {
                    AppError::Unavailable(format!("transcription failed: {message}"))
                })?,
            Err((_, message)) => {
                return Err(AppError::Unavailable(format!(
                    "transcription failed: {message}"
                )))
            }
        };

        Ok(interpret_whisper(&payload))
    }
}

/// Decide whether the provider actually heard speech.
pub fn interpret_whisper(payload: &WhisperResponse) -> Transcript {
    let text = payload.text.trim();
    if text.is_empty() {
        return Transcript::Empty;
    }
    if payload.duration > 0.0 && payload.duration < MIN_SPEECH_SECONDS {
        return Transcript::Empty;
    }

    if !payload.segments.is_empty() {
        let mut spoke = false;
        let mut repetition_only = true;
        let mut best_logprob = LOW_LOGPROB - 1.0;
        for segment in &payload.segments {
            if segment.no_speech_prob >= NO_SPEECH_THRESHOLD {
                continue;
            }
            spoke = true;
            if segment.compression_ratio <= REPEAT_RATIO {
                repetition_only = false;
            }
            if segment.avg_logprob > best_logprob {
                best_logprob = segment.avg_logprob;
            }
        }
        if !spoke || repetition_only {
            return Transcript::Empty;
        }
        // Very low confidence on a short output is a hallucinated phrase.
        if best_logprob <= LOW_LOGPROB && text.chars().count() <= SHORT_TEXT_RUNES {
            return Transcript::Empty;
        }
    }

    if is_hallucination_phrase(text) {
        return Transcript::Empty;
    }
    Transcript::Text(text.to_string())
}

pub const HALLUCINATION_PHRASES: &[&str] = &[
    "terima kasih",
    "terima kasih telah menonton",
    "terima kasih sudah menonton",
    "terima kasih telah menonton video ini",
    "terima kasih sudah menonton video ini",
    "terima kasih telah menonton sampai akhir",
    "selamat menonton",
    "jangan lupa subscribe",
    "jangan lupa like dan subscribe",
    "like dan subscribe",
    "subscribe",
    "sampai jumpa",
    "hai",
    "halo",
    "hello",
    "hi",
    "hmm",
    "you",
    "bye",
    "thank you",
    "thank you for watching",
    "thanks for watching",
    "something happen",
    "www youtube com",
];

/// Short transcripts only: a real sentence that happens to contain a thanks
/// must be kept.
pub fn is_hallucination_phrase(text: &str) -> bool {
    let normalized = normalize_speech_text(text);
    if normalized.is_empty() {
        return true;
    }
    if normalized.chars().count() > SHORT_TEXT_RUNES {
        return false;
    }
    HALLUCINATION_PHRASES
        .iter()
        .any(|phrase| repeats_phrase(&normalized, phrase))
}

/// Whole-word match once or repeated back to back, so "hai" does not match
/// "hidup" but "terima kasih terima kasih" is caught.
fn repeats_phrase(text: &str, phrase: &str) -> bool {
    if text == phrase {
        return true;
    }
    match text.strip_prefix(&format!("{phrase} ")) {
        Some(rest) => repeats_phrase(rest, phrase),
        None => false,
    }
}

pub fn normalize_speech_text(text: &str) -> String {
    let mut cleaned = String::with_capacity(text.len());
    for ch in text.to_lowercase().chars() {
        if ch.is_alphanumeric() {
            cleaned.push(ch);
        } else {
            cleaned.push(' ');
        }
    }
    cleaned.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub type SharedTranscriber = Arc<dyn Transcriber>;

#[cfg(test)]
mod tests;
