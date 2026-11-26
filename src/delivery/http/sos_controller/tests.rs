//! Media download headers: the stored filename is attacker-controlled.

use super::attachment_header;

#[test]
fn a_plain_name_is_quoted() {
    assert_eq!(
        attachment_header("cry.wav"),
        "attachment; filename=\"cry.wav\""
    );
}

#[test]
fn quotes_semicolons_and_controls_cannot_break_the_header() {
    let header = attachment_header("ev\"il; name\r\nX-Evil: 1.wav");
    assert_eq!(header, "attachment; filename=\"evil nameX-Evil: 1.wav\"");
    assert!(!header.contains('\r'));
    assert!(!header.contains('\n'));
    assert_eq!(header.matches('"').count(), 2);
}

#[test]
fn directory_components_are_dropped() {
    assert_eq!(
        attachment_header("../../etc/passwd"),
        "attachment; filename=\"passwd\""
    );
    assert_eq!(
        attachment_header("C:\\temp\\cry.wav"),
        "attachment; filename=\"cry.wav\""
    );
}

#[test]
fn non_ascii_is_stripped_with_a_fallback() {
    assert_eq!(
        attachment_header("rekaman suara.wav"),
        "attachment; filename=\"rekaman suara.wav\""
    );
    assert_eq!(
        attachment_header("日本語.wav"),
        "attachment; filename=\".wav\""
    );
    assert_eq!(attachment_header(""), "attachment; filename=\"download\"");
    assert_eq!(attachment_header(".."), "attachment; filename=\"download\"");
}
