use crate::produce_message;
use rdkafka::producer::FutureProducer;
use rpc_core::types::RpcMetricsAggregated;

pub async fn produce_rpc_metrics_aggregated(
    producer: &FutureProducer,
    metrics: &RpcMetricsAggregated,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(metrics)?;
    produce_message(
        producer,
        "rpc.metrics.aggregated",
        &metrics.provider,
        &payload,
    )
    .await
}
