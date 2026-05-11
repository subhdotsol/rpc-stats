use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::Utc;
use kafka::topics::tx_landed::produce_tx_landed;
use kafka::topics::tx_confirmed::produce_tx_confirmed;
use kafka::FutureProducer;
use rpc_core::types::{TxLanded, TxConfirmed};
use serde_json::Value;
use std::collections::HashMap;
use tokio_stream::StreamExt;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
};
use std::sync::Arc;

async fn run_subscription(
    endpoint: String,
    token: Option<String>,
    payer_pubkey: String,
    memo_program_id: String,
    commitment: CommitmentLevel,
    producer: Arc<FutureProducer>,
) -> Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(endpoint)?
        .x_token(token)?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;

    info!("Connected for commitment {:?}! Setting up subscription...", commitment);

    let mut transactions = HashMap::new();
    transactions.insert(
        "memo_txs".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![
                memo_program_id.clone(),
                payer_pubkey.clone(),
            ],
        },
    );

    let mut slots = HashMap::new();
    slots.insert(
        "slot_updates".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            interslot_updates: Some(false),
        },
    );

    let (_subscribe, mut stream) = client
        .subscribe_with_request(Some(SubscribeRequest {
            accounts: HashMap::new(),
            slots,
            transactions,
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: Some(commitment as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        }))
        .await?;

    info!("Subscribed to {:?}! Waiting for devnet transactions...", commitment);

    while let Some(update_res) = stream.next().await {
        let update = match update_res {
            Ok(u) => u,
            Err(e) => {
                error!("Stream error for {:?}: {:?}", commitment, e);
                break;
            }
        };

        if let Some(update_oneof) = update.update_oneof {
            match update_oneof {
                UpdateOneof::Transaction(tx_update) => {
                    if let Some(tx_info) = tx_update.transaction {
                        let signature = bs58::encode(&tx_info.signature).into_string();

                        if let Some(tx) = tx_info.transaction {
                            if let Some(message) = tx.message {
                                for ix in message.instructions {
                                    if ix.program_id_index as usize >= message.account_keys.len() {
                                        continue;
                                    }

                                    let program_id =
                                        &message.account_keys[ix.program_id_index as usize];
                                    let program_id_str =
                                        bs58::encode(program_id).into_string();

                                    if program_id_str == memo_program_id {
                                        let mut text =
                                            String::from_utf8(ix.data.clone()).ok();

                                        if text.is_none() {
                                            if let Ok(decoded) = STANDARD.decode(&ix.data) {
                                                text = String::from_utf8(decoded).ok();
                                            }
                                        }

                                        if let Some(content) = text {
                                            if let Ok(json) =
                                                serde_json::from_str::<Value>(&content)
                                            {
                                                let provider = json["provider"]
                                                    .as_str()
                                                    .unwrap_or("unknown");
                                                let timestamp = json["timestamp"]
                                                    .as_u64()
                                                    .unwrap_or(0);

                                                let now =
                                                    chrono::Utc::now().timestamp_millis()
                                                        as u64;
                                                let latency = now.saturating_sub(timestamp);

                                                match commitment {
                                                    CommitmentLevel::Processed => {
                                                        info!(
                                                            signature = %signature,
                                                            slot = tx_update.slot,
                                                            provider = %provider,
                                                            latency_ms = latency,
                                                            "Matched scheduler tx (PROCESSED)"
                                                        );

                                                        let kafka_tx = TxLanded {
                                                            signature: signature.clone(),
                                                            provider_id: provider.to_string(),
                                                            landed_slot: tx_update.slot as i64,
                                                            geyser_landed_at: Utc::now(),
                                                        };

                                                        if let Err(e) = produce_tx_landed(&producer, &kafka_tx).await {
                                                            error!(sig = %signature, "Failed to publish tx.landed to Kafka: {:?}", e);
                                                        }
                                                    }
                                                    CommitmentLevel::Confirmed => {
                                                        info!(
                                                            signature = %signature,
                                                            slot = tx_update.slot,
                                                            provider = %provider,
                                                            latency_ms = latency,
                                                            "Matched scheduler tx (CONFIRMED)"
                                                        );

                                                        let kafka_tx = TxConfirmed {
                                                            signature: signature.clone(),
                                                            provider_id: provider.to_string(),
                                                            rpc_confirmed_at: Utc::now(),
                                                        };

                                                        if let Err(e) = produce_tx_confirmed(&producer, &kafka_tx).await {
                                                            error!(sig = %signature, "Failed to publish tx.confirmed to Kafka: {:?}", e);
                                                        }
                                                    }
                                                    _ => {}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                UpdateOneof::Ping(_) => {
                    info!("Ping received ({:?})", commitment);
                }
                UpdateOneof::Pong(_) => {}
                _ => {}
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let endpoint = std::env::var("GRPC_ENDPOINT")
        .unwrap_or_else(|_| "https://aequa-solanad-d5c3.devnet.rpcpool.com".to_string());
    let token = std::env::var("X_TOKEN").ok();
    let memo_program_id = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

    info!("Geyser Consumer starting...");

    let config = rpc_core::config::Config::from_env().expect("Failed to load config");
    let producer = Arc::new(
        kafka::create_producer(&config.kafka_brokers)
    );

    let payer_pubkey = std::env::var("PAYER_PUBKEY")
        .unwrap_or_else(|_| "5GHnVhqZ6Yn8mQmM43CwMRTNWZofeyExMkP6PDGCAc9d".to_string());

    let (res1, res2) = tokio::join!(
        run_subscription(
            endpoint.clone(),
            token.clone(),
            payer_pubkey.clone(),
            memo_program_id.to_string(),
            CommitmentLevel::Processed,
            producer.clone(),
        ),
        run_subscription(
            endpoint.clone(),
            token.clone(),
            payer_pubkey.clone(),
            memo_program_id.to_string(),
            CommitmentLevel::Confirmed,
            producer.clone(),
        )
    );

    if let Err(e) = res1 {
        error!("Processed subscription error: {:?}", e);
    }
    if let Err(e) = res2 {
        error!("Confirmed subscription error: {:?}", e);
    }

    Ok(())
}
