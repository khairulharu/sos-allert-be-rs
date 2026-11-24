//! Whisper silence rules: silence must not become a transcript.

use super::*;

fn response(text: &str, duration: f64, segments: Vec<Segment>) -> WhisperResponse {
    WhisperResponse {
        text: text.to_string(),
        duration,
        segments,
    }
}

fn segment(no_speech: f64, logprob: f64, ratio: f64) -> Segment {
    Segment {
        no_speech_prob: no_speech,
        avg_logprob: logprob,
        compression_ratio: ratio,
    }
}

#[test]
fn real_speech_is_kept() {
    let payload = response(
        "tolong, bapak saya pingsan",
        3.0,
        vec![segment(0.05, -0.3, 1.1)],
    );
    match interpret_whisper(&payload) {
        Transcript::Text(text) => assert_eq!(text, "tolong, bapak saya pingsan"),
        Transcript::Empty => panic!("real speech dropped"),
    }
}

#[test]
fn empty_text_is_empty() {
    assert!(matches!(
        interpret_whisper(&response("   ", 2.0, vec![])),
        Transcript::Empty
    ));
}

#[test]
fn a_clip_shorter_than_half_a_second_is_empty() {
    assert!(matches!(
        interpret_whisper(&response("halo", 0.3, vec![])),
        Transcript::Empty
    ));
}

#[test]
fn every_segment_probably_silent_is_empty() {
    let payload = response(
        "terima kasih telah menonton",
        5.0,
        vec![segment(0.9, -1.5, 1.0), segment(0.8, -1.2, 1.0)],
    );
    assert!(matches!(interpret_whisper(&payload), Transcript::Empty));
}

#[test]
fn a_repetition_loop_is_empty() {
    // A single long segment with a compression ratio above the limit.
    let payload = response(
        "SOMETHING HAPPEN SOMETHING HAPPEN",
        4.0,
        vec![segment(0.1, -0.2, 3.1)],
    );
    assert!(matches!(interpret_whisper(&payload), Transcript::Empty));
}

#[test]
fn low_confidence_short_text_is_empty() {
    let payload = response("sesuatu", 2.0, vec![segment(0.2, -1.4, 1.0)]);
    assert!(matches!(interpret_whisper(&payload), Transcript::Empty));
}

#[test]
fn a_stock_subtitle_line_is_empty() {
    for text in [
        "Terima kasih telah menonton!",
        "terima kasih",
        "Jangan lupa subscribe",
        "www.youtube.com",
        "hai",
        "Thank you for watching",
        "terima kasih terima kasih",
    ] {
        assert!(
            matches!(
                interpret_whisper(&response(text, 6.0, vec![])),
                Transcript::Empty
            ),
            "{text:?} should be silence"
        );
    }
}

#[test]
fn a_real_sentence_containing_a_thanks_is_kept() {
    let text = "terima kasih tolong cepat, ibu saya sesak napas dan tidak sadar";
    match interpret_whisper(&response(text, 6.0, vec![])) {
        Transcript::Text(kept) => assert_eq!(kept, text),
        Transcript::Empty => panic!("real sentence dropped as a hallucination"),
    }
}

#[test]
fn whole_word_matching_does_not_eat_prefixes() {
    assert!(is_hallucination_phrase("hai"));
    assert!(!is_hallucination_phrase("hidup saya terancam"));
    assert!(is_hallucination_phrase("terima kasih terima kasih"));
    assert!(!is_hallucination_phrase(
        "terima kasih, sekarang tolong panggil ambulans"
    ));
}

#[test]
fn a_long_transcript_is_never_treated_as_a_hallucination_phrase() {
    let long = format!("terima kasih {}", "dan tolong".repeat(40));
    assert!(!is_hallucination_phrase(&long));
}

#[test]
fn text_normalisation_ignores_case_and_punctuation() {
    assert_eq!(
        normalize_speech_text("  Terima KASIH!!!  telah... menonton "),
        "terima kasih telah menonton"
    );
}

#[tokio::test]
async fn the_noop_transcriber_reports_not_configured() {
    let error = NoopTranscriber
        .transcribe("a.wav", "audio/wav", b"x")
        .await
        .unwrap_err();
    assert_eq!(error.status(), 412);
}
