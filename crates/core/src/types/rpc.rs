use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxSubmitted {
    pub signature: String,
    pub provider: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxLanded {
    pub signature: String,
    pub provider: String,
    pub slot: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxConfirmed {
    pub signature: String,
    pub provider: String,
    pub slot: u64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcMetricsRaw {
    pub provider: String,
    pub metric_name: String,
    pub value: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcMetricsAggregated {
    pub provider: String,
    pub metric_name: String,
    pub avg_value: f64,
    pub window_ms: u64,
    pub timestamp: DateTime<Utc>,
}
