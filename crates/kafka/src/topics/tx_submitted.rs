use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use rdkafka::producer::FutureProducer;
use crate::produce_message;

#[derive(Debug, Serialize, Deserialize)]
pub struct TxSubmitted {
    pub signature: String,
    pub provider: String,
    pub timestamp: DateTime<Utc>,
}

pub async fn produce_tx_submitted(
    producer: &FutureProducer,
    tx: &TxSubmitted,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(tx)?;
    produce_message(producer, "tx.submitted", &tx.signature, &payload).await
}
