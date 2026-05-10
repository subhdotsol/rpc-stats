use actix_web::{delete, get, post, put, web, HttpResponse};
use reqwest::Client as HttpClient;
use serde_json::json;
use tracing::{error, info};

use crate::app_state::AppState;
use crate::models::{
    AlertChannelRow, AlertLogRow, CreateChannelRequest, IncidentCreatedPayload,
    UpdateChannelRequest,
};
use crate::webhook;
use rpc_core::types::HealthResponse;

// ── Health ───────────────────────────────────────────────────────────────────

#[get("/internal/alerts/health")]
pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(HealthResponse { status: "ok" })
}

// ── Test alert ───────────────────────────────────────────────────────────────

#[post("/internal/alerts/test")]
pub async fn test_alert(state: web::Data<AppState>) -> HttpResponse {
    let channels =
        sqlx::query_as::<_, AlertChannelRow>("SELECT * FROM alert_channels WHERE enabled = TRUE")
            .fetch_all(&state.db)
            .await;

    let channels = match channels {
        Ok(c) => c,
        Err(e) => {
            error!("failed to load channels: {e}");
            return HttpResponse::InternalServerError()
                .json(json!({ "error": "failed to load channels" }));
        }
    };

    if channels.is_empty() {
        return HttpResponse::Ok().json(json!({
            "message": "No enabled alert channels configured — nothing to test"
        }));
    }

    let payload = IncidentCreatedPayload {
        incident_id: 0,
        provider_id: "test-provider".to_string(),
        incident_type: "degraded".to_string(),
        started_at: chrono::Utc::now(),
        description: Some("This is a test alert from RPC Stats Alert Engine".to_string()),
    };

    let http = HttpClient::new();
    webhook::fan_out_created(&http, &state.db, &channels, &payload).await;

    info!("test alert dispatched to {} channel(s)", channels.len());

    HttpResponse::Ok().json(json!({
        "message": format!("Test alert sent to {} channel(s)", channels.len())
    }))
}

// ── Channel CRUD ─────────────────────────────────────────────────────────────

#[get("/internal/alerts/channels")]
pub async fn list_channels(state: web::Data<AppState>) -> HttpResponse {
    match sqlx::query_as::<_, AlertChannelRow>(
        "SELECT * FROM alert_channels ORDER BY created_at DESC",
    )
    .fetch_all(&state.db)
    .await
    {
        Ok(rows) => HttpResponse::Ok().json(rows),
        Err(e) => {
            error!("list_channels: {e}");
            HttpResponse::InternalServerError().json(json!({ "error": "database error" }))
        }
    }
}

#[post("/internal/alerts/channels")]
pub async fn create_channel(
    state: web::Data<AppState>,
    body: web::Json<CreateChannelRequest>,
) -> HttpResponse {
    let valid_types = ["discord", "slack", "generic"];
    if !valid_types.contains(&body.channel_type.as_str()) {
        return HttpResponse::BadRequest().json(json!({
            "error": format!("channel_type must be one of: {}", valid_types.join(", "))
        }));
    }

    match sqlx::query_as::<_, AlertChannelRow>(
        r#"
        INSERT INTO alert_channels (name, channel_type, webhook_url)
        VALUES ($1, $2, $3)
        RETURNING *
        "#,
    )
    .bind(&body.name)
    .bind(&body.channel_type)
    .bind(&body.webhook_url)
    .fetch_one(&state.db)
    .await
    {
        Ok(row) => {
            info!(id = row.id, name = %row.name, "alert channel created");
            HttpResponse::Created().json(row)
        }
        Err(e) => {
            error!("create_channel: {e}");
            HttpResponse::InternalServerError().json(json!({ "error": "failed to create channel" }))
        }
    }
}

#[put("/internal/alerts/channels/{id}")]
pub async fn update_channel(
    state: web::Data<AppState>,
    path: web::Path<i64>,
    body: web::Json<UpdateChannelRequest>,
) -> HttpResponse {
    let id = path.into_inner();

    match sqlx::query_as::<_, AlertChannelRow>(
        r#"
        UPDATE alert_channels
        SET
            name        = COALESCE($1, name),
            webhook_url = COALESCE($2, webhook_url),
            enabled     = COALESCE($3, enabled),
            updated_at  = NOW()
        WHERE id = $4
        RETURNING *
        "#,
    )
    .bind(&body.name)
    .bind(&body.webhook_url)
    .bind(body.enabled)
    .bind(id)
    .fetch_optional(&state.db)
    .await
    {
        Ok(Some(row)) => {
            info!(id = row.id, name = %row.name, "alert channel updated");
            HttpResponse::Ok().json(row)
        }
        Ok(None) => HttpResponse::NotFound().json(json!({ "error": "channel not found" })),
        Err(e) => {
            error!("update_channel: {e}");
            HttpResponse::InternalServerError().json(json!({ "error": "failed to update channel" }))
        }
    }
}

#[delete("/internal/alerts/channels/{id}")]
pub async fn delete_channel(state: web::Data<AppState>, path: web::Path<i64>) -> HttpResponse {
    let id = path.into_inner();

    match sqlx::query("DELETE FROM alert_channels WHERE id = $1")
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(result) if result.rows_affected() > 0 => {
            info!(id, "alert channel deleted");
            HttpResponse::Ok().json(json!({ "message": "channel deleted" }))
        }
        Ok(_) => HttpResponse::NotFound().json(json!({ "error": "channel not found" })),
        Err(e) => {
            error!("delete_channel: {e}");
            HttpResponse::InternalServerError().json(json!({ "error": "failed to delete channel" }))
        }
    }
}

// ── Alert history ────────────────────────────────────────────────────────────

#[get("/internal/alerts/history")]
pub async fn alert_history(state: web::Data<AppState>) -> HttpResponse {
    match sqlx::query_as::<_, AlertLogRow>(
        r#"
        SELECT * FROM alert_log
        ORDER BY sent_at DESC
        LIMIT 100
        "#,
    )
    .fetch_all(&state.db)
    .await
    {
        Ok(rows) => HttpResponse::Ok().json(rows),
        Err(e) => {
            error!("alert_history: {e}");
            HttpResponse::InternalServerError().json(json!({ "error": "database error" }))
        }
    }
}
