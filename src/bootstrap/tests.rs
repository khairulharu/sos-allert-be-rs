//! The embedded migration set must stay the 15 reversible pairs: sqlx yields
//! both directions, so the down half must not be counted (or applied).

use sqlx::migrate::MigrationType;

#[test]
fn the_embedded_migration_set_is_the_up_files_only() {
    let migrator = super::embedded_migrator();
    let ups: Vec<_> = migrator
        .iter()
        .filter(|migration| migration.migration_type != MigrationType::ReversibleDown)
        .collect();

    let names: Vec<String> = ups.iter().map(|m| m.description.to_string()).collect();
    assert_eq!(names.len(), 15, "expected 15 migrations: {names:?}");
    assert_eq!(
        names.first().map(String::as_str),
        Some("create table users")
    );
    assert!(names.iter().any(|name| name == "alert dispatch outbox"));

    let versions: Vec<i64> = ups.iter().map(|m| m.version).collect();
    let mut sorted = versions.clone();
    sorted.sort_unstable();
    assert_eq!(versions, sorted, "applied in version order");

    // Every up file has its down counterpart.
    let downs = migrator
        .iter()
        .filter(|migration| migration.migration_type == MigrationType::ReversibleDown)
        .count();
    assert_eq!(downs, 15);
}
