use crate::produce_message;
use rdkafka::producer::FutureProducer;
use rpc_core::types::TxLanded;

pub async fn produce_tx_landed(producer: &FutureProducer, tx: &TxLanded) -> anyhow::Result<()> {
    let payload = serde_json::to_string(tx)?;
    produce_message(producer, "tx.landed", &tx.signature, &payload).await
}
