use crate::produce_message;
use rdkafka::producer::FutureProducer;
use rpc_core::types::TxSubmitted;

pub async fn produce_tx_submitted(
    producer: &FutureProducer,
    tx: &TxSubmitted,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(tx)?;
    produce_message(producer, "tx.submitted", &tx.signature, &payload).await
}
