use anyhow::Result;
use reqwest::Client;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::models::{AlertChannelRow, IncidentCreatedPayload, IncidentResolvedPayload};
use crate::webhook;

/// Spawn a long-lived task that uses Postgres LISTEN/NOTIFY to react to
/// incident lifecycle events in real time.
///
/// Channels listened:
///   - `incident_created`  (trigger: trg_incidents_notify)
///   - `incident_resolved` (trigger: trg_incidents_resolved_notify)
pub fn spawn_pg_listener(pool: PgPool) {
    tokio::spawn(async move {
        let http = Client::new();
        loop {
            if let Err(e) = listen_loop(&pool, &http).await {
                error!("pg listener crashed: {e:#}");
                warn!("reconnecting in 5s…");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    });
}

async fn listen_loop(pool: &PgPool, http: &Client) -> Result<()> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen_all(["incident_created", "incident_resolved"]).await?;

    info!("pg listener active on channels: incident_created, incident_resolved");

    loop {
        let notification = listener.recv().await?;

        // Load current channels from DB for each event (ensures new channels
        // are picked up without restart)
        let channels = load_enabled_channels(pool).await;

        match notification.channel() {
            "incident_created" => {
                match serde_json::from_str::<IncidentCreatedPayload>(notification.payload()) {
                    Ok(payload) => {
                        info!(incident_id = payload.incident_id, provider = %payload.provider_id, "incident_created received");
                        webhook::fan_out_created(http, pool, &channels, &payload).await;
                    }
                    Err(e) => warn!("failed to parse incident_created payload: {e}"),
                }
            }
            "incident_resolved" => {
                match serde_json::from_str::<IncidentResolvedPayload>(notification.payload()) {
                    Ok(payload) => {
                        info!(incident_id = payload.incident_id, provider = %payload.provider_id, "incident_resolved received");
                        webhook::fan_out_resolved(http, pool, &channels, &payload).await;
                    }
                    Err(e) => warn!("failed to parse incident_resolved payload: {e}"),
                }
            }
            other => {
                warn!("unexpected channel: {other}");
            }
        }
    }
}

/// Fetch all enabled alert channels from DB.
async fn load_enabled_channels(pool: &PgPool) -> Vec<AlertChannelRow> {
    sqlx::query_as::<_, AlertChannelRow>(
        "SELECT * FROM alert_channels WHERE enabled = TRUE ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_else(|e| {
        error!("failed to load alert channels: {e}");
        vec![]
    })
}
