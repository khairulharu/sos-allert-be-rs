//! Offline classifier parity table, ported case for case from
//! `TestOfflineClassifier` / `TestOfflineClassifierTokenMatching` in the Go
//! service.

use super::*;

fn check(description: &str, want_type: &str, want_severity: f64) {
    let result = classify(description);
    assert_eq!(
        result.emergency_type, want_type,
        "type for {description:?}: {result:?}"
    );
    assert!(
        (result.severity - want_severity).abs() < 1e-9,
        "severity for {description:?}: got {} want {want_severity}",
        result.severity
    );
    if want_type.is_empty() {
        assert!(result.summary.is_empty(), "summary for no signal");
    } else {
        assert!(!result.summary.is_empty(), "summary for {description:?}");
    }
    if want_severity >= 0.8 && !result.summary.is_empty() {
        assert!(
            result.summary.starts_with("PRIORITAS TINGGI: "),
            "high severity must be labelled: {}",
            result.summary
        );
    }
}

#[test]
fn empty_and_unknown_text_is_no_signal() {
    check("", "", 0.0);
    check("mau beli kopi dulu", "", 0.0);
}

#[test]
fn medical_signals() {
    check("ada orang pingsan di pantai", EMERGENCY_TYPE_MEDICAL, 0.6);
    check(
        "bapak saya sakit jantung, sesak napas",
        EMERGENCY_TYPE_MEDICAL,
        0.6,
    );
    check("anak tenggelam di sungai", EMERGENCY_TYPE_MEDICAL, 0.8);
    check(
        "bapak kejang-kejang, hampir tidak sadarkan diri",
        EMERGENCY_TYPE_MEDICAL,
        1.0,
    );
    check(
        "someone is bleeding, unconscious",
        EMERGENCY_TYPE_MEDICAL,
        0.8,
    );
}

#[test]
fn accident_signals() {
    check(
        "kecelakaan motor di jalan raya",
        EMERGENCY_TYPE_ACCIDENT,
        0.6,
    );
    check("KECELAKAAN DI JALAN", EMERGENCY_TYPE_ACCIDENT, 0.6);
    check("truk terbalik masuk jurang", EMERGENCY_TYPE_ACCIDENT, 0.6);
    check(
        "tabrakan beruntun, banyak korban",
        EMERGENCY_TYPE_ACCIDENT,
        0.8,
    );
}

#[test]
fn danger_signals_include_disasters() {
    check("diculik orang tak dikenal", EMERGENCY_TYPE_DANGER, 0.6);
    check("diancam pakai pisau, darurat!", EMERGENCY_TYPE_DANGER, 0.8);
    check("kebakaran di rumah tetangga", EMERGENCY_TYPE_DANGER, 0.8);
    check("banjir bandang di kampung kami", EMERGENCY_TYPE_DANGER, 0.8);
    check("gempa bumi, plafon rumah retak", EMERGENCY_TYPE_DANGER, 0.8);
    check("dicolek orang di kereta", EMERGENCY_TYPE_DANGER, 0.6);
    check("tetangga dibacok pencuri", EMERGENCY_TYPE_DANGER, 0.6);
    check("fires near the market", EMERGENCY_TYPE_DANGER, 0.6);
}

#[test]
fn severity_is_capped_and_ties_go_to_medical() {
    check(
        "kecelakaan motor, diancam, darurat, kritis, banyak korban",
        EMERGENCY_TYPE_ACCIDENT,
        1.0,
    );
    check(
        "kecelakaan, teman saya pingsan",
        EMERGENCY_TYPE_MEDICAL,
        0.6,
    );
}

#[test]
fn stretched_typing_and_suffixes_still_match() {
    check("tolonggg, anak saya pingsannn", EMERGENCY_TYPE_MEDICAL, 0.6);
    check(
        "kecelakaannya banyak, korban luka",
        EMERGENCY_TYPE_ACCIDENT,
        0.8,
    );
}

#[test]
fn false_positive_guards() {
    // Everything below must be no signal.
    for text in [
        "besok rapat pukul 10 pagi",
        "saya makan grape dan apel",
        "mati lampu di rumah",
        "beli SIM card baru",
        "ATM card saya tertelan",
        "motorik halus anak saya bagus",
        "grape juice enak",
    ] {
        let result = classify(text);
        assert_eq!(
            result.emergency_type, "",
            "{text:?} was classified as {result:?}"
        );
        assert_eq!(result.severity, 0.0);
    }
}

#[test]
fn summary_quotes_the_matched_evidence() {
    let result = classify("kebakaran di rumah tetangga");
    assert!(
        result.summary.contains("kebakaran"),
        "summary: {}",
        result.summary
    );
}

#[test]
fn token_helpers() {
    assert!(token_matches("pingsannya", "pingsan"));
    assert!(token_matches("pingsannn", "pingsan"));
    assert!(!token_matches("motorik", "motor"));
    assert!(!token_matches("card", "car"));
    assert_eq!(
        normalize_tokens("Kecelakaan, motor!"),
        vec!["kecelakaan", "motor"]
    );
    assert_eq!(squeeze_repeats("tolonggg"), "tolong");
}

#[test]
fn radius_start_index_follows_severity() {
    assert_eq!(radius_start_index(0.0, 3), 0);
    assert_eq!(radius_start_index(0.4, 3), 1);
    assert_eq!(radius_start_index(0.6, 3), 1);
    assert_eq!(radius_start_index(0.99, 3), 2);
    assert_eq!(radius_start_index(1.0, 3), 2);
    assert_eq!(radius_start_index(0.5, 1), 0);
}

#[tokio::test]
async fn engine_trait_returns_a_signal_for_a_known_phrase() {
    let engine = OfflineClassifier;
    let result = engine.triage("ada orang pingsan").await;
    assert!(result.is_signal());
    assert_eq!(result.emergency_type, EMERGENCY_TYPE_MEDICAL);
    let info = result.into_info();
    assert_eq!(info.emergency_type, EMERGENCY_TYPE_MEDICAL);
}
