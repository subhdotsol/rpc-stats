use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct LeaderboardRow {
    pub provider_id: String,
    pub rank: i32,
    pub landing_rate: Option<f64>,
    pub avg_confirm_ms: Option<i32>,
    pub avg_slot_lag: Option<f64>,
    pub p95_latency_ms: Option<i32>,
    pub avg_claim_vs_reality_ms: Option<i32>,
    pub uptime_24h: Option<f64>,
    pub status: Option<String>,
    pub last_tested_at: Option<DateTime<Utc>>,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct ProviderMetricsBucket {
    pub time: DateTime<Utc>,
    pub provider_id: String,
    pub landing_rate: Option<f64>,
    pub avg_confirm_ms: Option<i32>,
    pub avg_slot_lag: Option<f64>,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct LatestTestResult {
    pub signature: String,
    pub status: String,
    pub landing_time_ms: Option<i32>,
    pub landed_slot: Option<i64>,
    pub submitted_at: DateTime<Utc>,
    pub geyser_landed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct FeeBreakdownRow {
    pub fee_tier_id: String,
    pub lamports: i64,
    pub display_name: String,
    pub landing_rate: Option<f64>,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct RegionLatencyRow {
    pub region_id: String,
    pub display_name: String,
    pub avg_confirm_ms: Option<i32>,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct IncidentRow {
    pub id: i64,
    pub provider_id: String,
    pub incident_type: String,
    pub started_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub duration_seconds: Option<i32>,
    pub description: Option<String>,
    pub is_resolved: bool,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct RpcMethodRow {
    pub method_name: String,
    pub method_type: String,
    pub provider_id: String,
    pub p50_ms: Option<i32>,
    pub p95_ms: Option<i32>,
    pub p99_ms: Option<i32>,
    pub error_rate: Option<f64>,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct TrendStats {
    pub low_rate: Option<f64>,
    pub high_rate: Option<f64>,
    pub outage_count: Option<i32>,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct TestRunRow {
    pub signature: String,
    pub provider_id: String,
    pub status: String,
    pub landing_time_ms: Option<i32>,
    pub landed_slot: Option<i64>,
    pub geyser_landed_at: Option<DateTime<Utc>>,
    pub submitted_at: DateTime<Utc>,
    pub fee_tier_id: String,
    pub fee_lamports: Option<i64>,
    pub seconds_ago: Option<i32>,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct RegionProviderRow {
    pub region_name: String,
    pub provider_id: String,
    pub avg_latency_ms: Option<i32>,
}

#[derive(Debug, sqlx::FromRow, Serialize, Deserialize, Clone)]
pub struct RankHistoryRow {
    pub period: String,
    pub provider_id: String,
    pub rank: i32,
    pub composite_score: Option<f64>,
    pub landing_rate: Option<f64>,
    pub avg_confirm_ms: Option<i32>,
}
