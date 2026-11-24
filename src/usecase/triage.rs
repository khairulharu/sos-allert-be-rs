//! Emergency triage: deterministic offline classifier.
//!
//! Ported 1:1 from `ai_offline.go` (keyword tables generated from the Go
//! source, not retyped). The taxonomy is fixed at DANGER/MEDICAL/ACCIDENT
//! because the trigger request validates exactly those three; disasters map to
//! DANGER.
//!
//! Matching rules kept identical:
//! * lowercase, non-alphanumeric runes become separators, then tokens;
//! * runs of repeated letters are squeezed, so "tolonggg" matches "tolong";
//! * a token matches a keyword with one whole Indonesian/English suffix
//!   ("pingsannya"), and one-letter suffixes need a keyword of 4+ chars so
//!   "card" does not match "car";
//! * phrases must appear in order and adjacent.

use std::sync::Arc;

use async_trait::async_trait;

pub mod providers;

pub use providers::{AiTriage, JevClient, OllamaClient, OpenAiClient};

use crate::model::sos::TriageInfo;
use crate::usecase::triage_keywords as kw;

pub const EMERGENCY_TYPE_MEDICAL: &str = "MEDICAL";
pub const EMERGENCY_TYPE_ACCIDENT: &str = "ACCIDENT";
pub const EMERGENCY_TYPE_DANGER: &str = "DANGER";

pub const SEVERITY_BASE: f64 = 0.6;
pub const SEVERITY_BOOST: f64 = 0.2;
pub const SEVERITY_CAP: f64 = 1.0;
pub const STRONG_WEIGHT: i32 = 2;
pub const WEAK_WEIGHT: i32 = 1;

const WORD_SUFFIXES: &[&str] = &[
    "nya", "ku", "mu", "lah", "kah", "pun", "an", "i", "kan", "s", "es", "ed", "ing", "er", "ers",
    "d",
];
const MIN_KEYWORD_LEN_FOR_SHORT_SUFFIX: usize = 4;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TriageResult {
    pub emergency_type: String,
    pub severity: f64,
    pub summary: String,
}

impl TriageResult {
    /// A classifier with no signal is empty in every field.
    pub fn is_signal(&self) -> bool {
        !self.emergency_type.is_empty()
    }

    pub fn into_info(self) -> TriageInfo {
        TriageInfo {
            emergency_type: self.emergency_type,
            severity: self.severity,
            summary: self.summary,
        }
    }
}

#[async_trait]
pub trait Triage: Send + Sync {
    /// Never fails the caller: a provider error is the caller's cue to fall
    /// back, and the offline classifier never errors at all.
    async fn triage(&self, description: &str) -> TriageResult;
}

/// Deterministic keyword classifier. Always available, so the trigger has a
/// baseline classification even with every provider down.
#[derive(Debug, Default, Clone, Copy)]
pub struct OfflineClassifier;

#[async_trait]
impl Triage for OfflineClassifier {
    async fn triage(&self, description: &str) -> TriageResult {
        classify(description)
    }
}

struct CompiledKeyword {
    display: String,
    tokens: Vec<String>,
}

struct CompiledRule {
    kind: &'static str,
    strong: Vec<CompiledKeyword>,
    weak: Vec<CompiledKeyword>,
}

fn compile(items: &'static [&'static str]) -> Vec<CompiledKeyword> {
    items
        .iter()
        .filter_map(|keyword| {
            let tokens = normalize_tokens(keyword);
            if tokens.is_empty() {
                return None;
            }
            Some(CompiledKeyword {
                display: keyword.trim().to_lowercase(),
                tokens,
            })
        })
        .collect()
}

fn rules() -> Vec<CompiledRule> {
    vec![
        CompiledRule {
            kind: EMERGENCY_TYPE_MEDICAL,
            strong: compile(kw::MEDICAL_STRONG),
            weak: compile(kw::MEDICAL_WEAK),
        },
        CompiledRule {
            kind: EMERGENCY_TYPE_ACCIDENT,
            strong: compile(kw::ACCIDENT_STRONG),
            weak: compile(kw::ACCIDENT_WEAK),
        },
        CompiledRule {
            kind: EMERGENCY_TYPE_DANGER,
            strong: compile(kw::DANGER_STRONG),
            weak: compile(kw::DANGER_WEAK),
        },
    ]
}

/// Classify one description. Empty or unknown text is "no signal"
/// (`emergency_type` empty, severity 0).
pub fn classify(description: &str) -> TriageResult {
    if description.trim().is_empty() {
        return TriageResult::default();
    }
    let text = normalize_tokens(description);
    if text.is_empty() {
        return TriageResult::default();
    }

    let mut scores: Vec<(&'static str, i32)> = Vec::new();
    let mut evidence: Vec<(&'static str, String)> = Vec::new();

    for rule in rules() {
        let mut score = 0;
        for (keywords, weight) in [(&rule.strong, STRONG_WEIGHT), (&rule.weak, WEAK_WEIGHT)] {
            for keyword in keywords {
                if phrase_matches(&text, &keyword.tokens) {
                    score += weight;
                    if !evidence.iter().any(|(kind, _)| *kind == rule.kind) {
                        evidence.push((rule.kind, keyword.display.clone()));
                    }
                }
            }
        }
        if score > 0 {
            scores.push((rule.kind, score));
        }
    }

    if scores.is_empty() {
        return TriageResult::default();
    }

    let mut severity = SEVERITY_BASE;
    for keyword in compile(kw::URGENT) {
        if phrase_matches(&text, &keyword.tokens) {
            severity += SEVERITY_BOOST;
        }
    }
    severity = severity.min(SEVERITY_CAP);

    let kind = pick_emergency_type(&scores);
    let matched = evidence
        .iter()
        .find(|(k, _)| *k == kind)
        .map(|(_, display)| display.clone())
        .unwrap_or_default();

    TriageResult {
        emergency_type: kind.to_owned(),
        severity,
        summary: summarize(kind, severity, &matched),
    }
}

/// Highest weighted score wins; ties resolve by life-safety priority:
/// MEDICAL > DANGER > ACCIDENT.
fn pick_emergency_type(scores: &[(&'static str, i32)]) -> &'static str {
    let mut best = "";
    let mut best_score = 0;
    for kind in [
        EMERGENCY_TYPE_MEDICAL,
        EMERGENCY_TYPE_DANGER,
        EMERGENCY_TYPE_ACCIDENT,
    ] {
        if let Some((_, score)) = scores.iter().find(|(k, _)| *k == kind) {
            if *score > best_score {
                best = kind;
                best_score = *score;
            }
        }
    }
    best
}

pub(crate) fn summarize(kind: &str, severity: f64, evidence: &str) -> String {
    let mut line = match kind {
        EMERGENCY_TYPE_MEDICAL => "Kemungkinan darurat medis",
        EMERGENCY_TYPE_ACCIDENT => "Kemungkinan kecelakaan",
        _ => "Potensi bahaya atau ancaman",
    }
    .to_string();

    if !evidence.is_empty() {
        line.push_str(&format!(" ({evidence})"));
    }
    if severity >= 0.8 {
        return format!("PRIORITAS TINGGI: {line}");
    }
    line
}

/// Lowercase, turn non-alphanumeric runes into separators, split.
pub fn normalize_tokens(text: &str) -> Vec<String> {
    let mut cleaned = String::with_capacity(text.len());
    for ch in text.to_lowercase().chars() {
        if ch.is_alphanumeric() {
            cleaned.push(ch);
        } else {
            cleaned.push(' ');
        }
    }
    cleaned.split_whitespace().map(str::to_owned).collect()
}

/// Drop runs of identical letters: "pingsannn" -> "pingsan".
pub fn squeeze_repeats(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut prev: Option<char> = None;
    for ch in text.chars() {
        if Some(ch) == prev {
            continue;
        }
        out.push(ch);
        prev = Some(ch);
    }
    out
}

fn token_matches(text_token: &str, keyword_token: &str) -> bool {
    if matches_with_suffix(text_token, keyword_token) {
        return true;
    }
    matches_with_suffix(
        &squeeze_repeats(text_token),
        &squeeze_repeats(keyword_token),
    )
}

fn matches_with_suffix(text_token: &str, keyword_token: &str) -> bool {
    if text_token == keyword_token {
        return true;
    }
    let Some(rest) = text_token.strip_prefix(keyword_token) else {
        return false;
    };
    if !WORD_SUFFIXES.contains(&rest) {
        return false;
    }
    if rest.chars().count() == 1 && keyword_token.chars().count() < MIN_KEYWORD_LEN_FOR_SHORT_SUFFIX
    {
        return false;
    }
    true
}

fn phrase_matches(text: &[String], phrase: &[String]) -> bool {
    if phrase.is_empty() || text.len() < phrase.len() {
        return false;
    }
    for start in 0..=(text.len() - phrase.len()) {
        if phrase
            .iter()
            .enumerate()
            .all(|(offset, keyword)| token_matches(&text[start + offset], keyword))
        {
            return true;
        }
    }
    false
}

/// Radius start index from a severity score: broader for a worse incident.
pub fn radius_start_index(severity: f64, radii: usize) -> usize {
    if radii <= 1 || severity <= 0.0 {
        return 0;
    }
    if severity >= 1.0 {
        return radii - 1;
    }
    let index = (severity * radii as f64).floor() as usize;
    index.min(radii - 1)
}

pub type TriageEngine = Arc<dyn Triage>;

#[cfg(test)]
mod tests;
