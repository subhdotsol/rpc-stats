use rpc_core::{config::Config, types::rpc::{RpcProvider, SentTx}};
use serde_json::json;
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::{
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use std::{str::FromStr, sync::Arc, time::{SystemTime, UNIX_EPOCH}};
use tokio::time::{Duration, sleep};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn create_memo_ix(data: String) -> Instruction {
    Instruction {
        program_id: Pubkey::from_str(MEMO_PROGRAM_ID).unwrap(),
        accounts: vec![],
        data: data.into_bytes(),
    }
}

async fn send_tx(provider: RpcProvider, payer: Arc<Keypair>) -> anyhow::Result<SentTx> {
    let client =
        RpcClient::new_with_commitment(provider.url.clone(), CommitmentConfig::processed());

    let recent_blockhash = client.get_latest_blockhash()?;

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

    let signature = client.send_transaction(&tx)?;

    info!(
        provider = %provider.name,
        signature = %signature,
        "Transaction sent"
    );

    Ok(SentTx {
        signature: signature.to_string(),
        provider: provider.name,
        timestamp,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;

    info!(
        probe_interval_secs = config.probe_interval_secs,
        "Starting Prober"
    );

    let providers = vec![
        RpcProvider {
            name: "helius".to_string(),
            url: config.helius_rpc.clone(),
        },
        RpcProvider {
            name: "alchemy".to_string(),
            url: config.alchemy_rpc.clone(),
        },
    ];

    // Load payer keypair from the configured path
    let payer = Arc::new(
        solana_sdk::signature::read_keypair_file(
            shellexpand::tilde(&config.keypair_path).to_string(),
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );

    loop {
        let mut handles = vec![];

        for provider in providers.clone() {
            let payer = payer.clone();

            handles.push(tokio::spawn(async move {
                match send_tx(provider.clone(), payer).await {
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

        info!(sent = results.len(), "Probe batch complete");

        sleep(Duration::from_secs(config.probe_interval_secs)).await;
    }
}
