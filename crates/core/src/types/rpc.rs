use chrono::{DateTime, Utc};
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

/// Kafka event: tx.submitted
/// Published by the scheduler/worker service immediately after the RPC call.
/// Contains all fields needed by the ingestion service to INSERT into `transactions`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxSubmitted {
    pub signature: String,
    /// Maps to `providers.id` (e.g. "helius", "alchemy")
    pub provider_id: String,
    /// Maps to `regions.id` (e.g. "us-east-1")
    pub region_id: String,
    /// Maps to `fee_tiers.id` (e.g. "low", "medium", "high")
    pub fee_tier_id: String,
    pub submitted_at: DateTime<Utc>,
    pub submitted_slot: Option<i64>,
    /// Network TPS snapshot at submission time (from latest `network_conditions` row).
    pub network_tps: Option<i32>,
    /// Groups all providers tested in the same 30-second sweep.
    pub batch_id: String,
}

/// Kafka event: tx.landed
/// Published by the Geyser consumer when a memo tx arrives on-chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxLanded {
    pub signature: String,
    /// The provider that submitted this transaction.
    pub provider_id: String,
    /// The slot the transaction landed in (from Geyser).
    pub landed_slot: i64,
    /// Timestamp when Geyser reported landing (used to compute `landing_time_ms`).
    pub geyser_landed_at: DateTime<Utc>,
}

/// Kafka event: tx.confirmed
/// Published by the worker/scheduler when RPC polling reports confirmation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxConfirmed {
    pub signature: String,
    pub provider_id: String,
    pub rpc_confirmed_at: DateTime<Utc>,
}

/// Kafka event: tx.timeout
/// Published when a probe tx has not landed within the observation window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxTimeout {
    pub signature: String,
    pub provider_id: String,
    pub submitted_at: DateTime<Utc>,
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
