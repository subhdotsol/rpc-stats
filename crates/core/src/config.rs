use figment::{Figment, providers::Env};
use serde::Deserialize;

/// Application configuration loaded from environment variables.
///
/// All fields map directly to uppercase env var names (e.g. `HELIUS_RPC`,
/// `KEYPAIR_PATH`). Defaults are applied for optional fields.
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub helius_rpc: String,

    pub alchemy_rpc: String,

    /// Defaults to `~/.config/solana/id.json`.
    #[serde(default = "default_keypair_path")]
    pub keypair_path: String,

    /// Defaults to 30 seconds.
    #[serde(default = "default_probe_interval")]
    pub probe_interval_secs: u64,

    /// Kafka bootstrap broker list. Defaults to `localhost:9092`.
    #[serde(default = "default_kafka_brokers")]
    pub kafka_brokers: String,
}

fn default_keypair_path() -> String {
    "~/.config/solana/id.json".to_string()
}

fn default_probe_interval() -> u64 {
    30
}

fn default_kafka_brokers() -> String {
    "localhost:9092".to_string()
}

impl Config {
    /// Load config from environment variables.
    ///
    /// Variable names are the uppercase snake_case versions of each field
    /// (e.g. `HELIUS_RPC`, `ALCHEMY_RPC`, `KEYPAIR_PATH`, `PROBE_INTERVAL_SECS`).
    ///
    /// Call `dotenvy::dotenv().ok()` before this if you want `.env` file support.
    pub fn from_env() -> anyhow::Result<Self> {
        let config = Figment::new()
            .merge(Env::raw())
            .extract::<Self>()
            .map_err(|e| anyhow::anyhow!("Failed to load config from environment: {}", e))?;

        Ok(config)
    }
}
