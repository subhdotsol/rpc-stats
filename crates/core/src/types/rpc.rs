use serde::{ Deserialize, Serialize };
use uuid::Uuid;
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcType {
    Http,
    Websocket,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rpc {
    pub id: Uuid,
    pub chain_id: Uuid,
    pub provider: String,
    pub url: String,
    pub rpc_type: RpcType,
    pub is_active: bool,
    pub tags: serde_json::Value,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

// this is what gets pushed onto the Redis stream
// kept minimal — worker only needs what it needs to check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckJob {
    pub rpc_id: Uuid,
    pub url: String,
    pub rpc_type: RpcType,
    pub chain_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub rpc_id: Uuid,
    pub region: String,
    pub checked_by: Uuid,
    pub latency_ms: Option<i32>,
    pub block_number: Option<i64>,
    pub is_healthy: bool,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}
