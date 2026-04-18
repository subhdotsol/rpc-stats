use serde::{Deserialize, Serialize};

/// Represents a single RPC provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcProvider {
    /// provider name (e.g. "helius", "alchemy")
    pub name: String,

    /// RPC endpoint URL including API key if required
    pub url: String,
}

/// The result of a successfully sent probe transaction via an RPC provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentTx {
    /// Base58-encoded transaction signature
    pub signature: String,

    /// Name of the RPC provider that accepted the transaction
    pub provider: String,

    /// Unix timestamp in milliseconds when the transaction was submitted
    pub timestamp: u128,
}


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
