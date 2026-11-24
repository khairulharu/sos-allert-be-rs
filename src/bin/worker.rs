//! `worker` binary: Kafka consumers (entity mirror topics + SOS fan-out).

use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

use sos_allert::bootstrap::build_dispatcher;
use sos_allert::config::Config;
use sos_allert::delivery::messaging::consumer::{
    run_consumer, ConsumerOptions, ConsumerSettings, DeadLetter,
};
use sos_allert::delivery::messaging::handlers::{
    EntityEventConsumer, EntityKind, SosDispatchConsumer,
};
use sos_allert::gateway::kafka::KafkaPublisher;
use sos_allert::gateway::publisher::EventPublisher;
use sos_allert::model::events::topics;
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
    let brokers = config.kafka.bootstrap.servers.clone();
    let base_group = config.kafka.group.id.clone();
    let reset = config.kafka.auto.offset.reset.clone();
    let retries = config.kafka.consumer.max_retries;
    let backoff = Duration::from_secs(config.kafka.consumer.retry_backoff_seconds);

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut tasks = Vec::new();

    // Entity topics: best-effort mirrors, one group per topic so a rebalance
    // on one cannot disturb the others.
    let entity_options = ConsumerOptions {
        max_retries: retries,
        backoff,
        dead_letter: None,
    };
    for (topic, kind) in [
        (topics::USERS, EntityKind::User),
        (topics::CONTACTS, EntityKind::Contact),
        (topics::ADDRESSES, EntityKind::Address),
    ] {
        let settings = ConsumerSettings {
            brokers: brokers.clone(),
            group: format!("{base_group}-{topic}"),
            auto_offset_reset: reset.clone(),
        };
        let handler = Arc::new(EntityEventConsumer::new(kind));
        let options = entity_options.clone();
        let shutdown = shutdown_rx.clone();
        tasks.push(tokio::spawn(async move {
            consume_forever(settings, topic, handler, options, shutdown).await;
        }));
    }

    // sos.dispatched keeps the legacy base group id on purpose: a new group
    // would replay every historical event and re-notify old alerts.
    let dead_letter = if config.kafka.producer.enabled && !config.kafka.dlq.topic.is_empty() {
        match KafkaPublisher::new(&brokers, Duration::from_secs(10)) {
            Ok(producer) => Some(Arc::new(DeadLetter::new(
                Arc::new(producer) as Arc<dyn EventPublisher>,
                config.kafka.dlq.topic.clone(),
            ))),
            Err(e) => {
                tracing::warn!(error = %e, "dead-letter producer unavailable; failures are dropped");
                None
            }
        }
    } else {
        tracing::warn!(
            "kafka.producer.enabled is false: a failed SOS message is dropped after its retries"
        );
        None
    };

    // The worker owns the fan-out when the web process publishes: it resolves
    // device tokens and the sender's emergency contacts from the same database.
    let db = PgPoolOptions::new()
        .max_connections(config.database.pool.max)
        .acquire_timeout(Duration::from_secs(5))
        .connect_lazy(&config.database.dsn())
        .map_err(|e| anyhow::anyhow!("postgres pool: {e}"))?;
    let dispatcher = Arc::new(build_dispatcher(&config, db));
    let sos_handler = Arc::new(SosDispatchConsumer::new(dispatcher));
    let sos_settings = ConsumerSettings {
        brokers: brokers.clone(),
        group: base_group.clone(),
        auto_offset_reset: reset.clone(),
    };
    let sos_options = ConsumerOptions {
        max_retries: retries,
        backoff,
        dead_letter,
    };
    let shutdown = shutdown_rx.clone();
    tasks.push(tokio::spawn(async move {
        consume_forever(
            sos_settings,
            topics::SOS_DISPATCHED,
            sos_handler,
            sos_options,
            shutdown,
        )
        .await;
    }));

    tracing::info!(
        brokers = %brokers,
        group = %base_group,
        "worker started with 4 consumers"
    );

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received");
    let _ = shutdown_tx.send(true);

    for task in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
    }
    tracing::info!("worker stopped");
    Ok(())
}

/// Restart a consumer that ended because a dead-letter write failed: the group
/// replays from the last committed offset, so nothing is lost.
async fn consume_forever(
    settings: ConsumerSettings,
    topic: &'static str,
    handler: Arc<dyn sos_allert::delivery::messaging::consumer::MessageHandler>,
    options: ConsumerOptions,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        match run_consumer(
            ConsumerSettings {
                brokers: settings.brokers.clone(),
                group: settings.group.clone(),
                auto_offset_reset: settings.auto_offset_reset.clone(),
            },
            topic,
            handler.clone(),
            options.clone(),
            shutdown.clone(),
        )
        .await
        {
            Ok(()) => {
                if *shutdown.borrow() {
                    return;
                }
            }
            Err(e) => {
                tracing::error!(topic, error = %e, "consumer stopped; restarting from the last commit");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}
