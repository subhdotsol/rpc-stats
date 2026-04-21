use anyhow::Result;
use rpc_cache::{keys, set_json_ex, RedisPool};
use rpc_core::types::db_models::IncidentRow;
use sqlx::PgPool;
use tracing::{error, info};

#[derive(sqlx::FromRow)]
struct BreachRow {
    provider_id: String,
    incident_type: Option<String>,
    trigger_metric: Option<String>,
    trigger_value: Option<f64>,
}

/// Detect providers that are breaching quality thresholds in the last ~5 min,
/// and open a new incident row for each unresolved (provider, type) pair.
pub async fn detect_incidents(pool: &PgPool, redis: &RedisPool) -> Result<()> {
    // 1. Find providers breaching thresholds in the last 5 minutes
    let breaches: Vec<BreachRow> = sqlx::query_as(
        r#"
        SELECT
          provider_id,
          CASE
            WHEN landing_rate < 0.80   THEN 'outage'
            WHEN landing_rate < 0.92   THEN 'degraded'
            WHEN p95_latency_ms > 2000 THEN 'degraded'
          END AS incident_type,
          CASE
            WHEN landing_rate < 0.80   THEN 'landing_rate'
            WHEN landing_rate < 0.92   THEN 'landing_rate'
            WHEN p95_latency_ms > 2000 THEN 'p95_latency'
          END AS trigger_metric,
          CASE
            WHEN landing_rate < 0.80   THEN landing_rate
            WHEN landing_rate < 0.92   THEN landing_rate
            WHEN p95_latency_ms > 2000 THEN p95_latency_ms::DECIMAL
          END AS trigger_value
        FROM provider_metrics_5m
        WHERE time >= NOW() - INTERVAL '6 minutes'
          AND time = (
            SELECT MAX(time) FROM provider_metrics_5m pm2
            WHERE pm2.provider_id  = provider_metrics_5m.provider_id
              AND pm2.region_id    IS NULL
              AND pm2.fee_tier_id  IS NULL
          )
          AND region_id   IS NULL
          AND fee_tier_id IS NULL
          AND (
            landing_rate   < 0.92
            OR p95_latency_ms > 2000
          )
        "#,
    )
    .fetch_all(pool)
    .await?;

    let count = breaches.len();

    for breach in breaches {
        // Only insert if no active incident of the same type already exists for this provider
        sqlx::query(
            r#"
            INSERT INTO incidents (provider_id, incident_type, started_at, trigger_metric, trigger_value, description)
            SELECT $1, $2, NOW(), $3, $4,
              FORMAT('Auto-detected: %s = %s', $3, $4::TEXT)
            WHERE NOT EXISTS (
              SELECT 1 FROM incidents
              WHERE provider_id    = $1
                AND is_resolved    = FALSE
                AND incident_type  = $2
            )
            "#,
        )
        .bind(&breach.provider_id)
        .bind(&breach.incident_type)
        .bind(&breach.trigger_metric)
        .bind(&breach.trigger_value)
        .execute(pool)
        .await?;
    }

    // 2. Query active incidents and push to hot read path (Redis)
    let active_incidents = sqlx::query_as::<_, IncidentRow>(
        "SELECT * FROM incidents WHERE is_resolved = FALSE ORDER BY started_at DESC"
    )
    .fetch_all(pool)
    .await?;

    set_json_ex(
        redis,
        keys::INCIDENTS_ACTIVE,
        &active_incidents,
        keys::INCIDENTS_ACTIVE_TTL,
    )
    .await?;

    info!("detect_incidents: checked {count} breaches, synced active incidents to Redis");
    Ok(())
}

/// Spawn a background task that runs incident detection every `interval_secs`.
pub fn spawn_detect_incidents(pool: PgPool, redis: RedisPool, interval_secs: u64) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
        loop {
            ticker.tick().await;
            if let Err(e) = detect_incidents(&pool, &redis).await {
                error!("detect_incidents failed: {e:#}");
            }
        }
    });
}

