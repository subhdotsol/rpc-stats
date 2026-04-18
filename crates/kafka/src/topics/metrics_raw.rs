use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use rdkafka::producer::FutureProducer;
use crate::produce_message;

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcMetricsRaw {
    pub provider: String,
    pub metric_name: String,
    pub value: f64,
    pub timestamp: DateTime<Utc>,
}

pub async fn produce_rpc_metrics_raw(
    producer: &FutureProducer,
    metrics: &RpcMetricsRaw,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(metrics)?;
    produce_message(producer, "rpc.metrics.raw", &metrics.provider, &payload).await
}
