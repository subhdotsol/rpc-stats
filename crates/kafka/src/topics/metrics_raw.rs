use crate::produce_message;
use rdkafka::producer::FutureProducer;
use rpc_core::types::RpcMetricsRaw;

pub async fn produce_rpc_metrics_raw(
    producer: &FutureProducer,
    metrics: &RpcMetricsRaw,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(metrics)?;
    produce_message(producer, "rpc.metrics.raw", &metrics.provider, &payload).await
}
