use actix_web::{get, web, HttpResponse};
use rpc_core::types::api::SummaryResponse;
use serde::{Deserialize, Serialize};

use crate::app_state::AppState;
use crate::error::ApiError;

#[derive(Serialize, Deserialize, sqlx::FromRow)]
struct StatusCount {
    status: Option<String>,
    count: Option<i64>,
}

#[derive(Serialize, Deserialize, sqlx::FromRow)]
struct IncidentCount {
    count: Option<i64>,
}

#[derive(Serialize, Deserialize, sqlx::FromRow)]
struct AvgLatency {
    avg: Option<f64>,
}

#[get("/summary")]
async fn get_summary(state: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    // Basic aggregation
    // Query 1: Total RPCs and their statuses from leaderboard_current
    let statuses = sqlx::query_as::<_, StatusCount>(
        r#"
        SELECT status, COUNT(*) as count 
        FROM leaderboard_current 
        GROUP BY status
        "#
    )
    .fetch_all(&state.db)
    .await?;

    let mut total = 0;
    let mut healthy = 0;
    let mut unhealthy = 0;

    for s in statuses {
        let count = s.count.unwrap_or(0);
        total += count;
        if let Some(status_str) = s.status {
            if status_str == "healthy" {
                healthy += count;
            } else {
                unhealthy += count;
            }
        }
    }

    // Query 2: Active incidents
    let incidents_record = sqlx::query_as::<_, IncidentCount>(
        r#"
        SELECT COUNT(*) as count 
        FROM incidents 
        WHERE is_resolved = FALSE
        "#
    )
    .fetch_one(&state.db)
    .await?;

    // Query 3: Average latency across healthy RPCs (or overall)
    let avg_record = sqlx::query_as::<_, AvgLatency>(
        r#"
        SELECT AVG(avg_confirm_ms)::FLOAT as avg 
        FROM leaderboard_current
        "#
    )
    .fetch_one(&state.db)
    .await?;

    let avg_latency = match avg_record.avg {
        Some(val) => val as i32,
        None => 0
    };

    Ok(HttpResponse::Ok().json(SummaryResponse {
        total_rpcs: total as i32,
        healthy_rpcs: healthy as i32,
        unhealthy_rpcs: unhealthy as i32,
        active_incidents: incidents_record.count.unwrap_or(0) as i32,
        avg_latency_ms: avg_latency,
    }))
}

#[derive(Serialize, Deserialize, sqlx::FromRow)]
struct NetworkConditionRow {
    pub time: chrono::DateTime<chrono::Utc>,
    pub current_tps: i32,
    pub current_slot: i64,
    pub congestion_level: String,
}

#[get("/network/current")]
async fn get_network_current(state: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    let key = rpc_cache::keys::NETWORK_CURRENT;

    // 1. Try Redis
    if let Some(data) = rpc_cache::get_json::<NetworkConditionRow>(&state.redis, key).await {
        return Ok(HttpResponse::Ok().json(data));
    }

    // 2. Fallback to DB
    let row = sqlx::query_as::<_, NetworkConditionRow>(
        r#"
        SELECT time, current_tps, current_slot, congestion_level
        FROM network_conditions
        ORDER BY time DESC
        LIMIT 1
        "#
    )
    .fetch_optional(&state.db)
    .await?;

    if let Some(r) = row {
        // 3. Write-back
        let _ = rpc_cache::set_json_ex(
            &state.redis,
            key,
            &r,
            rpc_cache::keys::NETWORK_CURRENT_TTL
        ).await;
        Ok(HttpResponse::Ok().json(r))
    } else {
        Err(ApiError::NotFound("no network conditions recorded yet".to_string()))
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_summary).service(get_network_current);
}

