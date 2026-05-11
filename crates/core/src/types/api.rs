use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;


#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub data: T,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiErrorResponse {
    pub error: ApiErrorDetail,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiErrorDetail {
    pub code: String,
    pub message: String,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct LeaderboardResponse {
    pub window: String,
    pub data: Vec<LeaderboardEntry>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaderboardEntry {
    pub provider: String,
    pub rank: i32,
    pub landing_rate: f64,
    pub avg_confirm: i32,
    pub slot_lag: f64,
    pub p95_latency: i32,
    pub claim_vs_reality: i32,
    pub uptime24h: f64,
    pub status: String,
    pub trend: String,
    pub trend_data: Vec<f64>,
    pub last_tested_at: Option<DateTime<Utc>>,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct RpcListResponse {
    pub data: Vec<RpcListItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcListItem {
    pub id: String,
    pub provider: String,
    pub url: String,
    pub chain_id: String,
    pub is_active: bool,
    pub tags: HashMap<String, String>,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct RpcDetailResponse {
    pub id: String,
    pub provider: String,
    pub url: String,
    pub stats: RpcStats,
    pub health: RpcHealth,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcStats {
    pub success_rate: f64,
    pub avg_latency_ms: i32,
    pub avg_block_lag: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcHealth {
    pub status: String,
    pub last_checked_at: DateTime<Utc>,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct TimeseriesResponse {
    pub rpc_id: String,
    pub points: Vec<TimeseriesPoint>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimeseriesPoint {
    pub timestamp: DateTime<Utc>,
    pub avg_latency_ms: i32,
    pub avg_block_lag: f64,
    pub success_rate: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IncidentItem {
    pub id: String,
    pub rpc_id: String,
    pub region: Option<String>,
    pub reason: String,
    pub started_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i32>,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct SummaryResponse {
    pub total_rpcs: i32,
    pub healthy_rpcs: i32,
    pub unhealthy_rpcs: i32,
    pub active_incidents: i32,
    pub avg_latency_ms: i32,
}
