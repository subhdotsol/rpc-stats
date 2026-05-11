use actix_web::{get, web, HttpResponse};
use rpc_core::types::api::ApiResponse;
use rpc_core::types::db_models::{RankHistoryRow, RegionProviderRow, RpcMethodRow, TestRunRow};
use serde::Deserialize;

use crate::app_state::AppState;
use crate::error::ApiError;

#[derive(Deserialize)]
pub struct RpcMethodQuery {
    pub rpc_type: Option<String>,
}

#[get("/benchmarks/rpc-methods")]
async fn get_rpc_methods(
    state: web::Data<AppState>,
    query: web::Query<RpcMethodQuery>,
) -> Result<HttpResponse, ApiError> {
    let method_type = query.rpc_type.clone();

    // 1. Try Redis for global unfiltered view
    if method_type.is_none() {
        if let Some(data) = rpc_cache::get_json::<Vec<RpcMethodRow>>(&state.redis, rpc_cache::keys::BENCHMARKS_RPC_METHODS).await {
            return Ok(HttpResponse::Ok().json(ApiResponse { data }));
        }
    }

    // 2. Fallback to DB
    let rows = sqlx::query_as::<_, RpcMethodRow>(
        r#"
        SELECT
          method_name,
          method_type,
          provider_id,
          ROUND(AVG(p50_ms))::INT  AS p50_ms,
          ROUND(AVG(p95_ms))::INT  AS p95_ms,
          ROUND(AVG(p99_ms))::INT  AS p99_ms,
          AVG(error_rate)::FLOAT   AS error_rate
        FROM rpc_method_metrics
        WHERE time >= NOW() - INTERVAL '1 hour'
          AND ($1::TEXT IS NULL OR method_type = $1)
        GROUP BY method_name, method_type, provider_id
        ORDER BY method_name, provider_id
        "#
    )
    .bind(method_type.clone())
    .fetch_all(&state.db)
    .await?;

    // 3. Write-back if unfiltered
    if method_type.is_none() {
        let _ = rpc_cache::set_json_ex(
            &state.redis,
            rpc_cache::keys::BENCHMARKS_RPC_METHODS,
            &rows,
            rpc_cache::keys::BENCHMARKS_RPC_METHODS_TTL
        ).await;
    }

    Ok(HttpResponse::Ok().json(ApiResponse { data: rows }))
}

#[get("/benchmarks/multi-region")]
async fn get_multi_region(state: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    let rows = sqlx::query_as::<_, RegionProviderRow>(
        r#"
        SELECT
          r.display_name       AS region_name,
          m.provider_id,
          ROUND(AVG(m.avg_confirm_ms))::INT AS avg_latency_ms
        FROM regions r
        JOIN provider_metrics_5m m ON m.region_id = r.id
        WHERE m.time >= NOW() - INTERVAL '30 minutes'
          AND m.fee_tier_id IS NULL
        GROUP BY r.display_name, m.provider_id, r.id
        ORDER BY r.id, m.provider_id
        "#
    )
    .fetch_all(&state.db)
    .await?;

    Ok(HttpResponse::Ok().json(ApiResponse { data: rows }))
}

#[get("/rank-history")]
async fn get_rank_history(state: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    let rows = sqlx::query_as::<_, RankHistoryRow>(
        r#"
        SELECT DISTINCT ON (period, provider_id)
          period,
          provider_id,
          rank,
          composite_score::FLOAT,
          landing_rate::FLOAT,
          avg_confirm_ms
        FROM rank_snapshots
        ORDER BY period, provider_id, snapshot_at DESC
        "#
    )
    .fetch_all(&state.db)
    .await?;

    Ok(HttpResponse::Ok().json(ApiResponse { data: rows }))
}

#[derive(Deserialize)]
pub struct TestRunsQuery {
    pub limit: Option<i64>,
}

#[get("/test-runs")]
async fn get_test_runs(
    state: web::Data<AppState>,
    query: web::Query<TestRunsQuery>,
) -> Result<HttpResponse, ApiError> {
    let limit = query.limit.unwrap_or(20);

    // 1. Try Redis for default limit
    if limit == 20 {
        if let Some(data) = rpc_cache::get_json::<Vec<TestRunRow>>(&state.redis, rpc_cache::keys::TEST_RUNS_LATEST).await {
            return Ok(HttpResponse::Ok().json(ApiResponse { data }));
        }
    }

    // 2. Fallback to DB
    let rows = sqlx::query_as::<_, TestRunRow>(
        r#"
        SELECT
          r.signature,
          r.provider_id,
          r.status,
          r.landing_time_ms,
          r.landed_slot,
          r.geyser_landed_at,
          t.submitted_at,
          t.fee_tier_id,
          ft.lamports          AS fee_lamports,
          EXTRACT(EPOCH FROM (NOW() - COALESCE(r.geyser_landed_at, r.updated_at)))::INT
                               AS seconds_ago
        FROM tx_results r
        JOIN transactions t  ON t.id = r.transaction_id
        JOIN fee_tiers ft    ON ft.id = t.fee_tier_id
        WHERE r.status IN ('landed', 'dropped', 'timeout')
        ORDER BY r.updated_at DESC
        LIMIT $1
        "#
    )
    .bind(limit)
    .fetch_all(&state.db)
    .await?;

    // 3. Write-back for default limit
    if limit == 20 {
        let _ = rpc_cache::set_json_ex(
            &state.redis,
            rpc_cache::keys::TEST_RUNS_LATEST,
            &rows,
            rpc_cache::keys::TEST_RUNS_LATEST_TTL
        ).await;
    }

    Ok(HttpResponse::Ok().json(ApiResponse { data: rows }))
}


#[get("/benchmarks/fees")]
async fn get_fees(state: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    #[derive(sqlx::FromRow, serde::Serialize)]
    struct FeeBreakdownRow {
        fee_tier: String,
        provider_id: String,
        avg_confirm_ms: i32,
    }

    let rows = sqlx::query_as::<_, FeeBreakdownRow>(
        r#"
        SELECT
          ft.display_name AS fee_tier,
          m.provider_id,
          ROUND(AVG(m.avg_confirm_ms))::INT AS avg_confirm_ms
        FROM fee_tiers ft
        JOIN provider_metrics_5m m ON m.fee_tier_id = ft.id
        WHERE m.time >= NOW() - INTERVAL '1 hour'
          AND m.region_id IS NULL
        GROUP BY ft.display_name, m.provider_id, ft.lamports
        ORDER BY ft.lamports ASC, m.provider_id
        "#
    )
    .fetch_all(&state.db)
    .await?;

    Ok(HttpResponse::Ok().json(ApiResponse { data: rows }))
}


pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_rpc_methods)
        .service(get_multi_region)
        .service(get_rank_history)
        .service(get_test_runs)
        .service(get_fees);
}
