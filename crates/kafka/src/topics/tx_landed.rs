use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use rdkafka::producer::FutureProducer;
use crate::produce_message;

#[derive(Debug, Serialize, Deserialize)]
pub struct TxLanded {
    pub signature: String,
    pub provider: String,
    pub slot: u64,
    pub timestamp: DateTime<Utc>,
}

pub async fn produce_tx_landed(
    producer: &FutureProducer,
    tx: &TxLanded,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(tx)?;
    produce_message(producer, "tx.landed", &tx.signature, &payload).await
}
