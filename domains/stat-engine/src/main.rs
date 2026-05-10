use anyhow::Context;
use kafka::create_consumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::Message;
use rpc_core::config::Config;
use rpc_core::types::{TxConfirmed, TxLanded, TxSubmitted, TxTimeout};
use sqlx::postgres::PgPoolOptions;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use stat_engine::aggregator;
use stat_engine::state;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()),
        )
        .init();

    info!("Stat Engine: initializing…");

    let config = Config::from_env().context("failed to load config")?;

    // ── Postgres ─────────────────────────────────────────────────────────────
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await
        .context("failed to connect to Postgres")?;

    info!("Stat Engine: connected to Postgres");

    // ── Redis ────────────────────────────────────────────────────────────────
    let redis = rpc_cache::create_pool(&config.redis_url)?;

    info!("Stat Engine: connected to Redis");

    // ── Kafka consumer (own consumer group, independent from ingestion) ──────
    let consumer = create_consumer(&config.kafka_brokers, "stat-engine-group")
        .context("failed to create Kafka consumer")?;

    let topics = ["tx.submitted", "tx.landed", "tx.confirmed", "tx.timeout"];
    consumer
        .subscribe(&topics)
        .context("failed to subscribe to topics")?;

    info!("Stat Engine: subscribed to {:?}", topics);

    // ── In-memory store ──────────────────────────────────────────────────────
    let store = state::new_store();

    // ── Background tasks ─────────────────────────────────────────────────────
    aggregator::spawn_flush_task(store.clone(), pool.clone(), redis.clone());
    aggregator::spawn_cleanup_task(store.clone());

    info!("Stat Engine: flush (60s) and cleanup (300s) tasks spawned");
    info!("Stat Engine: entering consumer loop");

    // ── Main consumer loop ───────────────────────────────────────────────────
    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {e}"),
            Ok(m) => {
                let payload = match m.payload() {
                    Some(p) => match std::str::from_utf8(p) {
                        Ok(s) => s,
                        Err(_) => continue,
                    },
                    None => continue,
                };

                match m.topic() {
                    "tx.submitted" => match serde_json::from_str::<TxSubmitted>(payload) {
                        Ok(event) => state::handle_submitted(&store, event),
                        Err(e) => error!("parse tx.submitted: {e}"),
                    },
                    "tx.landed" => match serde_json::from_str::<TxLanded>(payload) {
                        Ok(event) => state::handle_landed(&store, event),
                        Err(e) => error!("parse tx.landed: {e}"),
                    },
                    "tx.confirmed" => match serde_json::from_str::<TxConfirmed>(payload) {
                        Ok(event) => state::handle_confirmed(&store, event),
                        Err(e) => error!("parse tx.confirmed: {e}"),
                    },
                    "tx.timeout" => match serde_json::from_str::<TxTimeout>(payload) {
                        Ok(event) => state::handle_timeout(&store, event),
                        Err(e) => error!("parse tx.timeout: {e}"),
                    },
                    other => warn!("unknown topic: {other}"),
                }

                if let Err(e) = consumer.commit_message(&m, CommitMode::Async) {
                    warn!("commit failed: {e}");
                }
            }
        }
    }
}
