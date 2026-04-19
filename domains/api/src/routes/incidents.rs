use actix_web::{get, web, HttpResponse};
use rpc_core::types::api::{ApiResponse, IncidentItem};
use rpc_core::types::db_models::IncidentRow;
use serde::Deserialize;

use crate::app_state::AppState;
use crate::error::ApiError;

#[derive(Deserialize)]
pub struct IncidentQuery {
    pub active: Option<bool>,
    pub rpc_id: Option<String>,
    pub region: Option<String>, // Maybe not in incidents DB, but could filter if applicable
    pub days: Option<i32>,
}

#[get("")]
async fn get_incidents(
    state: web::Data<AppState>,
    query: web::Query<IncidentQuery>,
) -> Result<HttpResponse, ApiError> {
    let days = query.days.unwrap_or(7);
    let active = query.active;
    let rpc_id = query.rpc_id.clone();

    // Reconcile coworker structure and LLM's query
    // If active=true, filter by is_resolved=false.
    // Assuming regions apply generally or we ignore it if not in schema.

    let interval = format!("{} days", days);

    // Using query builder approach or raw sql. Since we have dynamic filters like rpc_id,
    // we can use standard query_as with COALESCE to ignore them if null.
    let rows = sqlx::query_as::<_, IncidentRow>(
        r#"
        SELECT
          id,
          provider_id,
          incident_type,
          started_at,
          resolved_at,
          duration_seconds,
          description,
          is_resolved
        FROM incidents
        WHERE started_at >= NOW() - $1::INTERVAL
          AND ($2::BOOLEAN IS NULL OR is_resolved = NOT $2)
          AND ($3::TEXT IS NULL OR provider_id = $3)
        ORDER BY started_at DESC
        "#
    )
    .bind(interval)
    .bind(active)
    .bind(rpc_id)
    .fetch_all(&state.db)
    .await?;

    let data: Vec<IncidentItem> = rows
        .into_iter()
        .map(|r| IncidentItem {
            id: r.id.to_string(),
            rpc_id: r.provider_id,
            region: None, // Based on schema
            reason: r.incident_type,
            started_at: r.started_at,
            resolved_at: r.resolved_at,
            duration_ms: r.duration_seconds.map(|ds| ds * 1000), // convert to ms
        })
        .collect();

    Ok(HttpResponse::Ok().json(ApiResponse { data }))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/incidents").service(get_incidents));
}
