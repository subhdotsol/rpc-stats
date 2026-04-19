use actix_web::{get, web, HttpResponse};
use chrono::Utc;
use rpc_core::types::api::{LeaderboardEntry, LeaderboardResponse};
use rpc_core::types::db_models::LeaderboardRow;
use serde::Deserialize;

use crate::app_state::AppState;
use crate::error::ApiError;

#[derive(Deserialize)]
pub struct LeaderboardQuery {
    pub window: Option<String>,
    pub region: Option<String>,
    pub chain_id: Option<String>,
}

#[get("/leaderboard")]
async fn get_leaderboard(
    state: web::Data<AppState>,
    query: web::Query<LeaderboardQuery>,
) -> Result<HttpResponse, ApiError> {
    let window = query.window.clone().unwrap_or_else(|| "24h".to_string());
    if !["1m", "5m", "1h", "24h"].contains(&window.as_str()) {
        return Err(ApiError::InvalidQuery(
            "window must be one of 1m, 5m, 1h, 24h".to_string(),
        ));
    }

    let rows = sqlx::query_as::<_, LeaderboardRow>(
        r#"
        SELECT
          provider_id,
          rank,
          landing_rate::FLOAT,
          avg_confirm_ms,
          avg_slot_lag::FLOAT,
          p95_latency_ms,
          avg_claim_vs_reality_ms,
          uptime_24h::FLOAT,
          status,
          last_tested_at
        FROM leaderboard_current
        ORDER BY rank ASC
        "#
    )
    .fetch_all(&state.db)
    .await?;

    let entries: Vec<LeaderboardEntry> = rows
        .into_iter()
        .map(|r| LeaderboardEntry {
            provider: r.provider_id,
            success_rate: r.landing_rate.unwrap_or(0.0),
            avg_latency_ms: r.avg_confirm_ms.unwrap_or(0),
            avg_block_lag: r.avg_slot_lag.unwrap_or(0.0),
            total_requests: 0, // Placeholder
            failed_requests: 0, // Placeholder
        })
        .collect();

    let response = LeaderboardResponse {
        window,
        data: entries,
        generated_at: Utc::now(),
    };

    Ok(HttpResponse::Ok().json(response))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_leaderboard);
}
