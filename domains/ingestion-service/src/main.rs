mod handlers;

use anyhow::Context;
use kafka::create_consumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::Message;
use rpc_core::config::Config;
use rpc_core::types::{TxSubmitted, TxLanded, TxConfirmed, TxTimeout};
use sqlx::postgres::PgPoolOptions;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    // 1. Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    info!("Ingestion Service: Initializing...");

    // 2. Load configuration
    let config = Config::from_env().context("Failed to load environment configuration")?;

    // 3. Initialize Postgres pool
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&config.database_url)
        .await
        .context("Failed to connect to Postgres")?;

    info!("Ingestion Service: Connected to Postgres");

    // 4. Initialize Kafka consumer
    let consumer = create_consumer(&config.kafka_brokers, "ingestion-service-group")
        .context("Failed to create Kafka consumer")?;

    let topics = ["tx.submitted", "tx.landed", "tx.confirmed", "tx.timeout"];
    consumer.subscribe(&topics).context("Failed to subscribe to topics")?;

    info!("Ingestion Service: Subscribed to topics: {:?}", topics);

    // 5. Main consumer loop
    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload() {
                    Some(p) => match std::str::from_utf8(p) {
                        Ok(s) => s,
                        Err(_) => continue,
                    },
                    None => continue,
                };

                let res = match m.topic() {
                    "tx.submitted" => {
                        match serde_json::from_str::<TxSubmitted>(payload) {
                            Ok(tx) => handlers::tx_submitted::handle(&pool, tx).await,
                            Err(e) => { error!("Failed to parse tx.submitted: {}", e); Ok(()) }
                        }
                    }

                    "tx.landed" => {
                        match serde_json::from_str::<TxLanded>(payload) {
                            Ok(tx) => handlers::tx_landed::handle(&pool, tx).await,
                            Err(e) => { error!("Failed to parse tx.landed: {}", e); Ok(()) }
                        }
                    }
                    "tx.confirmed" => {
                        match serde_json::from_str::<TxConfirmed>(payload) {
                            Ok(tx) => handlers::tx_confirmed::handle(&pool, tx).await,
                            Err(e) => { error!("Failed to parse tx.confirmed: {}", e); Ok(()) }
                        }
                    }
                    "tx.timeout" => {
                        match serde_json::from_str::<TxTimeout>(payload) {
                            Ok(tx) => handlers::tx_timeout::handle(&pool, tx).await,
                            Err(e) => { error!("Failed to parse tx.timeout: {}", e); Ok(()) }
                        }
                    }

                    _ => {
                        warn!("Received message from unknown topic: {}", m.topic());
                        Ok(())
                    }
                };

                if let Err(e) = res {
                    error!("Handler error for topic {}: {:#}", m.topic(), e);
                }

                if let Err(e) = consumer.commit_message(&m, CommitMode::Async) {
                    warn!("Failed to commit message: {}", e);
                }
            }
        }
    }
}
