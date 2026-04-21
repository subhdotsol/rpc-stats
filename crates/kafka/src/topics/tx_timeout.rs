use crate::produce_message;
use rdkafka::producer::FutureProducer;
use rpc_core::types::TxTimeout;

pub async fn produce_tx_timeout(
    producer: &FutureProducer,
    tx: &TxTimeout,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(tx)?;
    produce_message(producer, "tx.timeout", &tx.signature, &payload).await
}
