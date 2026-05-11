use actix_web::{get, web, HttpResponse};
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;

use crate::app_state::AppState;
use crate::error::ApiError;

#[derive(Deserialize)]
pub struct TimeseriesQuery {
    pub metric: String,
    pub period: String, // 7d, 30d, 90d
}

#[get("/trends/timeseries")]
async fn get_timeseries(
    state: web::Data<AppState>,
    query: web::Query<TimeseriesQuery>,
) -> Result<HttpResponse, ApiError> {
    // Determine table and interval based on period
    let (table, interval) = match query.period.as_str() {
        "7d" => ("provider_metrics_1h", "7 days"),
        "30d" => ("provider_metrics_1d", "30 days"),
        "90d" => ("provider_metrics_1d", "90 days"),
        _ => return Err(ApiError::InvalidQuery("Invalid period".to_string())),
    };

    let sql = format!(
        "SELECT time, provider_id, landing_rate::FLOAT, avg_confirm_ms, avg_slot_lag::FLOAT 
         FROM {} 
         WHERE time >= NOW() - INTERVAL '{}'
           AND region_id IS NULL AND fee_tier_id IS NULL
         ORDER BY time ASC",
        table, interval
    );

    #[derive(sqlx::FromRow)]
    struct Row {
        time: chrono::DateTime<Utc>,
        provider_id: String,
        landing_rate: Option<f64>,
        avg_confirm_ms: Option<i32>,
        avg_slot_lag: Option<f64>,
    }

    let rows: Vec<Row> = sqlx::query_as(&sql).fetch_all(&state.db).await?;

    let mut points_map: std::collections::BTreeMap<i64, serde_json::Value> = std::collections::BTreeMap::new();

    for row in rows {
        let ts = row.time.timestamp_millis();
        let point = points_map.entry(ts).or_insert_with(|| {
            json!({
                "time": row.time.to_rfc3339(),
            })
        });

        let value = match query.metric.as_str() {
            "landing_rate" => json!(row.landing_rate.unwrap_or(0.0) * 100.0),
            "latency" => json!(row.avg_confirm_ms.unwrap_or(0)),
            "slot_lag" => json!(row.avg_slot_lag.unwrap_or(0.0)),
            _ => json!(null),
        };

        if let Some(obj) = point.as_object_mut() {
            obj.insert(row.provider_id, value);
        }
    }

    let data: Vec<serde_json::Value> = points_map.into_values().collect();
    Ok(HttpResponse::Ok().json(data))
}

#[get("/trends/tps-correlation")]
async fn get_tps_correlation(state: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    #[derive(sqlx::FromRow)]
    struct TpsRow {
        time: chrono::DateTime<Utc>,
        provider_id: String,
        landing_rate: Option<f64>,
        avg_network_tps: Option<i32>,
    }

    let rows = sqlx::query_as::<_, TpsRow>(
        r#"
        SELECT time, provider_id, landing_rate::FLOAT, avg_network_tps
        FROM provider_metrics_5m
        WHERE time >= NOW() - INTERVAL '24 hours'
          AND region_id IS NULL AND fee_tier_id IS NULL
        ORDER BY time ASC
        "#
    )
    .fetch_all(&state.db)
    .await?;

    let mut points_map: std::collections::BTreeMap<i64, serde_json::Value> = std::collections::BTreeMap::new();

    for row in rows {
        let ts = row.time.timestamp_millis();
        let point = points_map.entry(ts).or_insert_with(|| {
            json!({
                "time": row.time.to_rfc3339(),
                "tps": row.avg_network_tps.unwrap_or(0),
            })
        });

        if let Some(obj) = point.as_object_mut() {
            obj.insert(row.provider_id, json!(row.landing_rate.unwrap_or(0.0) * 100.0));
        }
    }

    let data: Vec<serde_json::Value> = points_map.into_values().collect();
    Ok(HttpResponse::Ok().json(data))
}

#[get("/trends/rank-history")]
async fn get_rank_history(state: web::Data<AppState>) -> Result<HttpResponse, ApiError> {
    use rpc_core::types::db_models::RankHistoryRow;

    let rows = sqlx::query_as::<_, RankHistoryRow>(
        r#"
        SELECT period, provider_id, rank, composite_score::FLOAT, landing_rate::FLOAT, avg_confirm_ms
        FROM rank_snapshots
        WHERE snapshot_date = CURRENT_DATE
        "#
    )
    .fetch_all(&state.db)
    .await?;

    let mut map: std::collections::HashMap<String, serde_json::Value> = std::collections::HashMap::new();
    for row in rows {
        let entry = map.entry(row.period.clone()).or_insert_with(|| json!({ "period": row.period }));
        if let Some(obj) = entry.as_object_mut() {
            obj.insert(row.provider_id, json!(row.rank));
        }
    }
    
    // Sort array by predefined periods if possible or just return array
    let data: Vec<serde_json::Value> = map.into_values().collect();
    Ok(HttpResponse::Ok().json(data))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_timeseries);
    cfg.service(get_tps_correlation);
    cfg.service(get_rank_history);
}
