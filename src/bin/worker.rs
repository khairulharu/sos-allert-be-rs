//! `worker` binary: Kafka consumers plus the read-only fan-out lookups.
//!
//! Phase 0: it loads config, builds the same state as `web`, and idles until
//! SIGINT/SIGTERM. Consumers are registered in phase 5 together with the
//! retry/DLQ semantics.

use sos_allert::bootstrap;
use sos_allert::config::Config;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,sos_allert=debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();

    let config = Config::load()?;
    let kafka_servers = config.kafka.bootstrap.servers.clone();
    let group = config.kafka.group.id.clone();
    let _state = bootstrap::build_state(config)?;

    tracing::info!(
        brokers = %kafka_servers,
        group = %group,
        "worker started with no consumers registered (phase 5)"
    );

    tokio::signal::ctrl_c().await?;
    tracing::info!("worker stopped");
    Ok(())
}
