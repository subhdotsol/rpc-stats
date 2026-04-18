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
