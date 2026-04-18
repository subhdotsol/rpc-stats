use anyhow::Result;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::metadata::MetadataValue;
use tracing::{error, info, warn};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use base64::{engine::general_purpose::STANDARD, Engine};

pub mod geyser {
    tonic::include_proto!("geyser");
}

pub mod solana {
    pub mod storage {
        pub mod confirmed_block {
            tonic::include_proto!("solana.storage.confirmed_block");
        }
    }
}

use geyser::geyser_client::GeyserClient;
use geyser::{
    SubscribeRequest, SubscribeRequestFilterTransactions, CommitmentLevel, 
    SubscribeRequestFilterSlots, SubscribeUpdate
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    // Helius Devnet Geyser (LaserStream)
    let endpoint = "https://laserstream-devnet-ewr.helius-rpc.com:443";
    let token = std::env::var("X_TOKEN").unwrap_or_else(|_| "".to_string());
    let memo_program_id = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

    info!("Connecting to Helius Devnet Geyser at {}...", endpoint);

    let channel = Channel::from_shared(endpoint.to_string())?
        .tls_config(ClientTlsConfig::new())?
        .connect()
        .await?;

    let token_val = if !token.is_empty() {
        Some(MetadataValue::from_str(&token)?)
    } else {
        warn!("No X_TOKEN found in environment. Helius will reject the connection.");
        None
    };

    let mut client = GeyserClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
        if let Some(t) = &token_val {
            req.metadata_mut().insert("x-token", t.clone());
        }
        Ok(req)
    });

    let mut transactions = HashMap::new();
    transactions.insert("client".to_string(), SubscribeRequestFilterTransactions {
        vote: Some(false),
        failed: Some(false),
        signature: None,
        account_include: vec![memo_program_id.to_string()],
        account_exclude: vec![],
        account_required: vec![],
    });

    let mut slots = HashMap::new();
    slots.insert("client".to_string(), SubscribeRequestFilterSlots {
        filter_by_commitment: Some(true),
        interslot_updates: Some(false),
    });

    let request = SubscribeRequest {
        accounts: HashMap::new(),
        slots,
        transactions,
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    };

    let request_stream = tokio_stream::iter(vec![request]);
    let mut stream = client.subscribe(request_stream).await?.into_inner();

    info!("Subscribed! Waiting for Devnet data...");

    while let Some(update_res) = stream.next().await {
        let update: SubscribeUpdate = match update_res {
            Ok(u) => u,
            Err(e) => {
                error!("Stream error: {:?}", e);
                break;
            }
        };

        if let Some(update_oneof) = update.update_oneof {
            match update_oneof {
                geyser::subscribe_update::UpdateOneof::Slot(slot_update) => {
                    info!("Slot received: {} ({:?})", slot_update.slot, slot_update.status());
                }
                geyser::subscribe_update::UpdateOneof::Transaction(tx_update) => {
                    if let Some(tx_info) = tx_update.transaction {
                        let signature = bs58::encode(&tx_info.signature).into_string();

                        if let Some(tx) = tx_info.transaction {
                            if let Some(message) = tx.message {
                                for ix in message.instructions {
                                    if ix.program_id_index as usize >= message.account_keys.len() {
                                        continue;
                                    }

                                    let program_id = &message.account_keys[ix.program_id_index as usize];
                                    let program_id_str = bs58::encode(program_id).into_string();

                                    if program_id_str == memo_program_id {
                                        let mut text = String::from_utf8(ix.data.clone()).ok();
                                        
                                        if text.is_none() {
                                            if let Ok(decoded) = STANDARD.decode(&ix.data) {
                                                text = String::from_utf8(decoded).ok();
                                            }
                                        }

                                        if let Some(content) = text {
                                            if let Ok(json) = serde_json::from_str::<Value>(&content) {
                                                let provider = json["provider"].as_str().unwrap_or("unknown");
                                                let timestamp = json["timestamp"].as_u64().unwrap_or(0);

                                                let now = chrono::Utc::now().timestamp_millis() as u64;
                                                let latency = now.saturating_sub(timestamp);

                                                info!(
                                                    signature = %signature,
                                                    provider = %provider,
                                                    latency_ms = latency,
                                                    "🔥 Matched test tx"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                geyser::subscribe_update::UpdateOneof::Ping(_) => {
                    info!("Ping received");
                }
                _ => {}
            }
        }
    }

    Ok(())
}
