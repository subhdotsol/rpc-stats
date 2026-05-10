use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ── DB rows ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, sqlx::FromRow, Serialize, Deserialize)]
pub struct AlertChannelRow {
    pub id: i64,
    pub name: String,
    pub channel_type: String,
    pub webhook_url: String,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize, Deserialize)]
pub struct AlertLogRow {
    pub id: i64,
    pub incident_id: i64,
    pub channel_id: i64,
    pub event_type: String,
    pub status: String,
    pub error_message: Option<String>,
    pub sent_at: DateTime<Utc>,
}

// ── Postgres NOTIFY payloads ─────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct IncidentCreatedPayload {
    pub incident_id: i64,
    pub provider_id: String,
    pub incident_type: String,
    pub started_at: DateTime<Utc>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IncidentResolvedPayload {
    pub incident_id: i64,
    pub provider_id: String,
    pub incident_type: String,
    pub duration_seconds: Option<i32>,
    pub resolved_at: DateTime<Utc>,
}

// ── API request bodies ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateChannelRequest {
    pub name: String,
    pub channel_type: String,
    pub webhook_url: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateChannelRequest {
    pub name: Option<String>,
    pub webhook_url: Option<String>,
    pub enabled: Option<bool>,
}
