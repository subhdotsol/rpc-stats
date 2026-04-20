use actix_web::{get, web, HttpResponse};
use chrono::Utc;
use rpc_core::types::api::{
    ApiResponse, RpcDetailResponse, RpcHealth, RpcListItem,
    RpcListResponse, RpcStats, TimeseriesPoint, TimeseriesResponse,
};
use rpc_core::types::db_models::{
    FeeBreakdownRow, LatestTestResult, LeaderboardRow, ProviderMetricsBucket, RegionLatencyRow,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::app_state::AppState;
use crate::error::ApiError;

#[derive(Serialize, Deserialize, sqlx::FromRow)]
struct ProviderRow {
    id: String,
    display_name: String,
}

#[get("")]
async fn list_rpcs(state: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    let records = sqlx::query_as::<_, ProviderRow>(r#"SELECT id, display_name FROM providers"#)
        .fetch_all(&state.db)
        .await?;

    let data: Vec<RpcListItem> = records
        .into_iter()
        .map(|p| {
            let mut tags = HashMap::new();
            tags.insert("region".to_string(), "global".to_string());
            RpcListItem {
                id: p.id.clone(),
                provider: p.display_name.clone(),
                url: format!("https://rpc.{}.xyz", p.id.to_lowercase()),
                chain_id: "solana-mainnet".to_string(),
                is_active: true,
                tags,
            }
        })
        .collect();

    Ok(HttpResponse::Ok().json(RpcListResponse { data }))
}

#[get("/{id}")]
async fn get_rpc(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, ApiError> {
    let rpc_id = path.into_inner();

    let row = sqlx::query_as::<_, LeaderboardRow>(
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
        WHERE provider_id = $1
        "#
    )
    .bind(&rpc_id)
    .fetch_optional(&state.db)
    .await?;

    if let Some(r) = row {
        let response = RpcDetailResponse {
            id: r.provider_id.clone(),
            provider: r.provider_id.clone(),
            url: format!("https://rpc.{}.xyz", r.provider_id.to_lowercase()),
            stats: RpcStats {
                success_rate: r.landing_rate.unwrap_or(0.0),
                avg_latency_ms: r.avg_confirm_ms.unwrap_or(0),
                avg_block_lag: r.avg_slot_lag.unwrap_or(0.0),
            },
            health: RpcHealth {
                status: r.status.unwrap_or_else(|| "unknown".to_string()),
                last_checked_at: r.last_tested_at.unwrap_or_else(Utc::now),
            },
        };
        Ok(HttpResponse::Ok().json(response))
    } else {
        Err(ApiError::NotFound(format!("rpc {} does not exist", rpc_id)))
    }
}

#[derive(Deserialize)]
pub struct TimeseriesQuery {
    pub window: Option<String>,
    pub interval: Option<String>,
}

#[get("/{id}/timeseries")]
async fn get_rpc_timeseries(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<TimeseriesQuery>,
) -> Result<HttpResponse, ApiError> {
    let provider_id = path.into_inner();
    let window = query.window.clone().unwrap_or_else(|| "1h".to_string());

    let (interval_sql, bucket_pg, table) = match window.as_str() {
        "1h" => ("1 hour", "5 minutes", "provider_metrics_5m"),
        "6h" => ("6 hours", "5 minutes", "provider_metrics_5m"),
        "24h" => ("24 hours", "5 minutes", "provider_metrics_5m"),
        _ => {
            return Err(ApiError::InvalidQuery(
                "window does not exist".to_string(),
            ))
        }
    };

    let sql = format!(
        r#"
        SELECT
          time_bucket($1::interval, time) AS time,
          provider_id,
          (SUM(tx_landed)::DECIMAL / NULLIF(SUM(tx_submitted), 0))::FLOAT AS landing_rate,
          ROUND(AVG(avg_confirm_ms))::INT AS avg_confirm_ms,
          ROUND(AVG(avg_slot_lag), 2)::FLOAT AS avg_slot_lag
        FROM {}
        WHERE provider_id = $2
          AND region_id IS NULL
          AND fee_tier_id IS NULL
          AND time >= NOW() - $3::interval
        GROUP BY time_bucket($1::interval, time), provider_id
        ORDER BY 1 ASC
        "#,
        table
    );

    let rows: Vec<ProviderMetricsBucket> = sqlx::query_as(&sql)
        .bind(bucket_pg)
        .bind(&provider_id)
        .bind(interval_sql)
        .fetch_all(&state.db)
        .await?;

    let points: Vec<TimeseriesPoint> = rows
        .into_iter()
        .map(|r| TimeseriesPoint {
            timestamp: r.time,
            avg_latency_ms: r.avg_confirm_ms.unwrap_or(0),
            avg_block_lag: r.avg_slot_lag.unwrap_or(0.0),
            success_rate: r.landing_rate.unwrap_or(0.0),
        })
        .collect();

    Ok(HttpResponse::Ok().json(TimeseriesResponse {
        rpc_id: provider_id,
        points,
    }))
}

#[get("/{id}/fee-breakdown")]
async fn get_fee_breakdown(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, ApiError> {
    let provider_id = path.into_inner();
    let rows = sqlx::query_as::<_, FeeBreakdownRow>(
        r#"
        SELECT
          ft.id AS fee_tier_id,
          ft.lamports,
          ft.display_name,
          m.landing_rate::FLOAT
        FROM fee_tiers ft
        LEFT JOIN LATERAL (
          SELECT landing_rate
          FROM provider_metrics_5m
          WHERE provider_id = $1
            AND fee_tier_id = ft.id
            AND region_id IS NULL
            AND time >= NOW() - INTERVAL '10 minutes'
          ORDER BY time DESC
          LIMIT 1
        ) m ON true
        ORDER BY ft.sort_order ASC
        "#
    )
    .bind(provider_id)
    .fetch_all(&state.db)
    .await?;

    Ok(HttpResponse::Ok().json(ApiResponse { data: rows }))
}

#[get("/{id}/region-latency")]
async fn get_region_latency(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, ApiError> {
    let provider_id = path.into_inner();
    let rows = sqlx::query_as::<_, RegionLatencyRow>(
        r#"
        SELECT
          r.id           AS region_id,
          r.display_name,
          m.avg_confirm_ms
        FROM regions r
        LEFT JOIN LATERAL (
          SELECT avg_confirm_ms
          FROM provider_metrics_5m
          WHERE provider_id = $1
            AND region_id = r.id
            AND fee_tier_id IS NULL
            AND time >= NOW() - INTERVAL '10 minutes'
          ORDER BY time DESC
          LIMIT 1
        ) m ON true
        ORDER BY m.avg_confirm_ms ASC NULLS LAST
        "#
    )
    .bind(provider_id)
    .fetch_all(&state.db)
    .await?;

    Ok(HttpResponse::Ok().json(ApiResponse { data: rows }))
}

#[get("/{id}/latest-tests")]
async fn get_latest_tests(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, ApiError> {
    let provider_id = path.into_inner();
    let rows = sqlx::query_as::<_, LatestTestResult>(
        r#"
        SELECT
          r.signature,
          r.status,
          r.landing_time_ms,
          r.landed_slot,
          t.submitted_at,
          r.geyser_landed_at
        FROM tx_results r
        JOIN transactions t ON t.id = r.transaction_id
        WHERE r.provider_id = $1
          AND r.status IN ('landed', 'dropped', 'timeout')
        ORDER BY r.updated_at DESC
        LIMIT 5
        "#
    )
    .bind(provider_id)
    .fetch_all(&state.db)
    .await?;

    Ok(HttpResponse::Ok().json(ApiResponse { data: rows }))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/rpcs")
            .service(list_rpcs)
            .service(get_rpc)
            .service(get_rpc_timeseries)
            .service(get_fee_breakdown)
            .service(get_region_latency)
            .service(get_latest_tests),
    );
}
