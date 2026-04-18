use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use rdkafka::producer::FutureProducer;
use crate::produce_message;

#[derive(Debug, Serialize, Deserialize)]
pub struct TxConfirmed {
    pub signature: String,
    pub provider: String,
    pub slot: u64,
    pub timestamp: DateTime<Utc>,
}

pub async fn produce_tx_confirmed(
    producer: &FutureProducer,
    tx: &TxConfirmed,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(tx)?;
    produce_message(producer, "tx.confirmed", &tx.signature, &payload).await
}
