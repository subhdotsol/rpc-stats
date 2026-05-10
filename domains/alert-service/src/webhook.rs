use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::json;
use tracing::{error, info};

use crate::models::{AlertChannelRow, IncidentCreatedPayload, IncidentResolvedPayload};

/// Send an incident-created alert to a single channel.
pub async fn send_incident_created(
    client: &Client,
    channel: &AlertChannelRow,
    payload: &IncidentCreatedPayload,
) -> Result<()> {
    let body = match channel.channel_type.as_str() {
        "discord" => format_discord_created(payload),
        "slack" => format_slack_created(payload),
        "generic" => format_generic_created(payload),
        other => anyhow::bail!("unknown channel type: {other}"),
    };

    client
        .post(&channel.webhook_url)
        .json(&body)
        .send()
        .await
        .context("webhook POST failed")?
        .error_for_status()
        .context("webhook returned error status")?;

    info!(
        channel = %channel.name,
        incident_id = payload.incident_id,
        "incident_created alert sent"
    );
    Ok(())
}

/// Send an incident-resolved alert to a single channel.
pub async fn send_incident_resolved(
    client: &Client,
    channel: &AlertChannelRow,
    payload: &IncidentResolvedPayload,
) -> Result<()> {
    let body = match channel.channel_type.as_str() {
        "discord" => format_discord_resolved(payload),
        "slack" => format_slack_resolved(payload),
        "generic" => format_generic_resolved(payload),
        other => anyhow::bail!("unknown channel type: {other}"),
    };

    client
        .post(&channel.webhook_url)
        .json(&body)
        .send()
        .await
        .context("webhook POST failed")?
        .error_for_status()
        .context("webhook returned error status")?;

    info!(
        channel = %channel.name,
        incident_id = payload.incident_id,
        "incident_resolved alert sent"
    );
    Ok(())
}

// ── Discord formatting ───────────────────────────────────────────────────────

fn severity_color(incident_type: &str) -> u32 {
    match incident_type {
        "outage" => 0xFF0000,      // red
        "degraded" => 0xFFA500,    // orange
        "maintenance" => 0x3498DB, // blue
        _ => 0x95A5A6,             // grey
    }
}

fn format_discord_created(p: &IncidentCreatedPayload) -> serde_json::Value {
    let desc = p
        .description
        .as_deref()
        .unwrap_or("No details available");

    json!({
        "embeds": [{
            "title": format!("🚨 {} — {}", p.provider_id.to_uppercase(), p.incident_type.to_uppercase()),
            "description": desc,
            "color": severity_color(&p.incident_type),
            "fields": [
                { "name": "Provider", "value": &p.provider_id, "inline": true },
                { "name": "Severity", "value": &p.incident_type, "inline": true },
            ],
            "timestamp": p.started_at.to_rfc3339(),
            "footer": { "text": "RPC Stats Alert Engine" }
        }]
    })
}

fn format_discord_resolved(p: &IncidentResolvedPayload) -> serde_json::Value {
    let duration = p.duration_seconds.unwrap_or(0);
    let minutes = duration / 60;
    let seconds = duration % 60;

    json!({
        "embeds": [{
            "title": format!("✅ {} — RESOLVED", p.provider_id.to_uppercase()),
            "description": format!(
                "{} incident resolved after {}m {}s",
                p.incident_type, minutes, seconds
            ),
            "color": 0x2ECC71, // green
            "fields": [
                { "name": "Provider", "value": &p.provider_id, "inline": true },
                { "name": "Duration", "value": format!("{}m {}s", minutes, seconds), "inline": true },
            ],
            "timestamp": p.resolved_at.to_rfc3339(),
            "footer": { "text": "RPC Stats Alert Engine" }
        }]
    })
}

// ── Slack formatting ─────────────────────────────────────────────────────────

fn format_slack_created(p: &IncidentCreatedPayload) -> serde_json::Value {
    let desc = p
        .description
        .as_deref()
        .unwrap_or("No details available");
    let emoji = match p.incident_type.as_str() {
        "outage" => "🔴",
        "degraded" => "🟡",
        _ => "🔵",
    };

    json!({
        "blocks": [
            {
                "type": "header",
                "text": { "type": "plain_text", "text": format!("{emoji} {}: {}", p.provider_id.to_uppercase(), p.incident_type.to_uppercase()) }
            },
            {
                "type": "section",
                "text": { "type": "mrkdwn", "text": format!("*Provider:* `{}`\n*Severity:* `{}`\n*Details:* {}", p.provider_id, p.incident_type, desc) }
            },
            {
                "type": "context",
                "elements": [{ "type": "mrkdwn", "text": format!("RPC Stats Alert Engine • {}", p.started_at.to_rfc3339()) }]
            }
        ]
    })
}

fn format_slack_resolved(p: &IncidentResolvedPayload) -> serde_json::Value {
    let duration = p.duration_seconds.unwrap_or(0);
    let minutes = duration / 60;
    let seconds = duration % 60;

    json!({
        "blocks": [
            {
                "type": "header",
                "text": { "type": "plain_text", "text": format!("✅ {}: RESOLVED", p.provider_id.to_uppercase()) }
            },
            {
                "type": "section",
                "text": { "type": "mrkdwn", "text": format!("*Provider:* `{}`\n*Incident:* `{}`\n*Duration:* {}m {}s", p.provider_id, p.incident_type, minutes, seconds) }
            },
            {
                "type": "context",
                "elements": [{ "type": "mrkdwn", "text": format!("RPC Stats Alert Engine • {}", p.resolved_at.to_rfc3339()) }]
            }
        ]
    })
}

// ── Generic HTTP webhook ─────────────────────────────────────────────────────

fn format_generic_created(p: &IncidentCreatedPayload) -> serde_json::Value {
    json!({
        "event": "incident_created",
        "incident_id": p.incident_id,
        "provider_id": p.provider_id,
        "incident_type": p.incident_type,
        "description": p.description,
        "started_at": p.started_at.to_rfc3339(),
    })
}

fn format_generic_resolved(p: &IncidentResolvedPayload) -> serde_json::Value {
    json!({
        "event": "incident_resolved",
        "incident_id": p.incident_id,
        "provider_id": p.provider_id,
        "incident_type": p.incident_type,
        "duration_seconds": p.duration_seconds,
        "resolved_at": p.resolved_at.to_rfc3339(),
    })
}

// ── Fan-out helper ───────────────────────────────────────────────────────────

/// Dispatch an incident-created alert to ALL enabled channels and log results.
pub async fn fan_out_created(
    client: &Client,
    pool: &sqlx::PgPool,
    channels: &[AlertChannelRow],
    payload: &IncidentCreatedPayload,
) {
    for ch in channels.iter().filter(|c| c.enabled) {
        let (status, err_msg) = match send_incident_created(client, ch, payload).await {
            Ok(()) => ("sent", None),
            Err(e) => {
                error!(channel = %ch.name, error = %e, "webhook delivery failed");
                ("failed", Some(format!("{e:#}")))
            }
        };
        log_alert(pool, payload.incident_id, ch.id, "created", status, err_msg.as_deref()).await;
    }
}

/// Dispatch an incident-resolved alert to ALL enabled channels and log results.
pub async fn fan_out_resolved(
    client: &Client,
    pool: &sqlx::PgPool,
    channels: &[AlertChannelRow],
    payload: &IncidentResolvedPayload,
) {
    for ch in channels.iter().filter(|c| c.enabled) {
        let (status, err_msg) = match send_incident_resolved(client, ch, payload).await {
            Ok(()) => ("sent", None),
            Err(e) => {
                error!(channel = %ch.name, error = %e, "webhook delivery failed");
                ("failed", Some(format!("{e:#}")))
            }
        };
        log_alert(pool, payload.incident_id, ch.id, "resolved", status, err_msg.as_deref()).await;
    }
}

/// Insert a row into alert_log recording a delivery attempt.
async fn log_alert(
    pool: &sqlx::PgPool,
    incident_id: i64,
    channel_id: i64,
    event_type: &str,
    status: &str,
    error_message: Option<&str>,
) {
    let res = sqlx::query(
        r#"
        INSERT INTO alert_log (incident_id, channel_id, event_type, status, error_message)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(incident_id)
    .bind(channel_id)
    .bind(event_type)
    .bind(status)
    .bind(error_message)
    .execute(pool)
    .await;

    if let Err(e) = res {
        error!(incident_id, channel_id, "failed to insert alert_log: {e}");
    }
}
