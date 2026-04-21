use chrono::Utc;
use dashmap::DashMap;
use kafka::FutureProducer;
use kafka::topics::tx_submitted::produce_tx_submitted;
use rpc_core::types::rpc::{RpcProvider, SentTx, TxSubmitted};
use serde_json::json;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::{
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use std::{
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{error, info};
use uuid::Uuid;

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

pub fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

pub fn create_memo_ix(data: String) -> Instruction {
    Instruction {
        program_id: Pubkey::from_str(MEMO_PROGRAM_ID).unwrap(),
        accounts: vec![],
        data: data.into_bytes(),
    }
}

pub async fn send_tx(
    provider: RpcProvider,
    payer: Arc<Keypair>,
    producer: Arc<FutureProducer>,
    sent_map: Arc<DashMap<String, SentTx>>,
) -> anyhow::Result<SentTx> {
    let client =
        RpcClient::new_with_commitment(provider.url.clone(), CommitmentConfig::processed());

    let recent_blockhash = client.get_latest_blockhash().await?;

    let test_id = Uuid::new_v4().to_string();
    let timestamp = now_ms();

    let memo_payload = json!({
        "test_id": test_id,
        "provider": provider.name,
        "timestamp": timestamp
    })
    .to_string();

    let memo_ix = create_memo_ix(memo_payload);
    let message = Message::new(&[memo_ix], Some(&payer.pubkey()));
    let tx = Transaction::new(&[payer.as_ref()], message, recent_blockhash);

    let signature = client.send_transaction(&tx).await?;

    info!(
        provider = %provider.name,
        signature = %signature,
        "Transaction sent"
    );

    let sent_tx = SentTx {
        signature: signature.to_string(),
        provider: provider.name.clone(),
        timestamp,
    };

    // Store in shared map for tracking
    sent_map.insert(signature.to_string(), sent_tx.clone());

    // Produce to Kafka
    let kafka_tx = TxSubmitted {
        signature: signature.to_string(),
        provider_id: provider.name,
        region_id: "global".to_string(),   // TODO: pass through from config
        fee_tier_id: "low".to_string(),    // TODO: pass through from config
        submitted_at: Utc::now(),
        submitted_slot: None,              // TODO: capture from getSlot() call
        network_tps: None,                 // TODO: snapshot from network_conditions
        batch_id: Uuid::new_v4().to_string(),
    };

    if let Err(e) = produce_tx_submitted(&producer, &kafka_tx).await {
        eprintln!("Failed to produce to Kafka: {:?}", e);
    }


    Ok(sent_tx)
}

pub async fn run_batch(
    providers: Vec<RpcProvider>,
    payer: Arc<Keypair>,
    producer: Arc<FutureProducer>,
    sent_map: Arc<DashMap<String, SentTx>>,
) {
    let mut handles = vec![];

    for provider in providers {
        let payer = payer.clone();
        let producer = producer.clone();
        let sent_map = sent_map.clone();

        handles.push(tokio::spawn(async move {
            match send_tx(provider.clone(), payer, producer, sent_map).await {
                Ok(sent) => Some(sent),
                Err(e) => {
                    error!(provider = %provider.name, error = ?e, "Transaction failed");
                    None
                }
            }
        }));
    }

    let mut results = vec![];

    for h in handles {
        if let Ok(Some(res)) = h.await {
            results.push(res);
        }
    }

    info!(
        sent = results.len(),
        tracked = sent_map.len(),
        "Probe batch complete"
    );
}
