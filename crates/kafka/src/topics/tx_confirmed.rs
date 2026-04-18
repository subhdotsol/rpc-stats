use crate::produce_message;
use rdkafka::producer::FutureProducer;
use rpc_core::types::TxConfirmed;

pub async fn produce_tx_confirmed(
    producer: &FutureProducer,
    tx: &TxConfirmed,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(tx)?;
    produce_message(producer, "tx.confirmed", &tx.signature, &payload).await
}
