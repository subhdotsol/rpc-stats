use actix_web::{web, App, HttpServer};
use dashmap::DashMap;
use rpc_core::{config::Config, types::rpc::{RpcProvider, SentTx}};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::info;
use tracing_subscriber::EnvFilter;

mod scheduler;
mod handlers;

use crate::scheduler::{now_ms, run_batch};
use crate::handlers::{health, run_schedule, AppState};

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;
    let port = 7001;

    info!(
        probe_interval_secs = config.probe_interval_secs,
        port = port,
        "Starting Scheduler Service"
    );

    let producer = Arc::new(kafka::create_producer(&config.kafka_brokers));
    let sent_map: Arc<DashMap<String, SentTx>> = Arc::new(DashMap::new());

    // Preventing memory leak
    {
        let cleanup_map = sent_map.clone();
        tokio::spawn(async move {
            loop {
                let now = now_ms();
                cleanup_map.retain(|_, tx| now - tx.timestamp < 60_000);
                sleep(Duration::from_secs(10)).await;
            }
        });
    }

    let providers = vec![
        RpcProvider {
            name: "helius".to_string(),
            url: config.helius_rpc.clone(),
        },
        RpcProvider {
            name: "alchemy".to_string(),
            url: config.alchemy_rpc.clone(),
        },
        RpcProvider {
            name: "triton".to_string(),
            url: config.triton_rpc.clone(),
        },
    ];

    let payer = Arc::new(
        solana_sdk::signature::read_keypair_file(
            shellexpand::tilde(&config.keypair_path).to_string(),
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );

    let app_state = Arc::new(AppState {
        providers: providers.clone(),
        payer: payer.clone(),
        producer: producer.clone(),
        sent_map: sent_map.clone(),
    });

    // Background scheduler loop (every 30 seconds as requested)
    {
        let providers = providers.clone();
        let payer = payer.clone();
        let producer = producer.clone();
        let sent_map = sent_map.clone();

        tokio::spawn(async move {
            loop {
                info!("Scheduled probe run starting...");
                run_batch(providers.clone(), payer.clone(), producer.clone(), sent_map.clone()).await;
                sleep(Duration::from_secs(30)).await;
            }
        });
    }

    info!("Starting Actix server on port {}", port);
    
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(app_state.clone()))
            .service(health)
            .service(run_schedule)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await?;

    Ok(())
}
