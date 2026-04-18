use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use rdkafka::producer::FutureProducer;
use crate::produce_message;

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcMetricsAggregated {
    pub provider: String,
    pub metric_name: String,
    pub avg_value: f64,
    pub window_ms: u64,
    pub timestamp: DateTime<Utc>,
}

pub async fn produce_rpc_metrics_aggregated(
    producer: &FutureProducer,
    metrics: &RpcMetricsAggregated,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(metrics)?;
    produce_message(producer, "rpc.metrics.aggregated", &metrics.provider, &payload).await
}
