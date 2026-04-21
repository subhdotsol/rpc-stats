use anyhow::Result;
use sqlx::PgPool;
use tracing::{error, info};

/// Historical periods snapshotted into `rank_snapshots`.
///
/// Tuple layout: (period_label, lookback_interval_literal)
const PERIODS: &[(&str, &str)] = &[
    ("today", "0 days"),
    ("1d",    "1 day"),
    ("3d",    "3 days"),
    ("7d",    "7 days"),
    ("14d",   "14 days"),
    ("30d",   "30 days"),
    ("90d",   "90 days"),
];

/// Snapshot per-period composite rankings into `rank_snapshots`.
/// Uses `ON CONFLICT … DO NOTHING` so running it multiple times per day is safe.
pub async fn snapshot_rankings(pool: &PgPool) -> Result<()> {
    for (period_label, lookback) in PERIODS {
        sqlx::query(
            r#"
            INSERT INTO rank_snapshots (
              snapshot_at, period, provider_id, rank,
              composite_score, landing_rate, avg_confirm_ms, avg_slot_lag
            )
            WITH metrics AS (
              SELECT
                provider_id,
                AVG(landing_rate)   AS landing_rate,
                AVG(avg_confirm_ms) AS avg_confirm_ms,
                AVG(avg_slot_lag)   AS avg_slot_lag,
                (
                  AVG(landing_rate)   * 0.50
                  + (1.0 / NULLIF(AVG(avg_confirm_ms), 0)) * 30000.0 * 0.30
                  + (1.0 / NULLIF(AVG(avg_slot_lag),   0)) * 5.0     * 0.20
                ) AS composite_score
              FROM provider_metrics_1h
              WHERE region_id   IS NULL
                AND fee_tier_id IS NULL
                AND time >= NOW() - ($2 || '')::INTERVAL
              GROUP BY provider_id
            )
            SELECT
              NOW(),
              $1,
              provider_id,
              RANK() OVER (ORDER BY composite_score DESC)::INT,
              composite_score,
              landing_rate,
              ROUND(avg_confirm_ms)::INT,
              ROUND(avg_slot_lag, 2)
            FROM metrics
            ON CONFLICT (period, provider_id, snapshot_date) DO NOTHING
            "#,
        )
        .bind(period_label)
        .bind(lookback)
        .execute(pool)
        .await?;
    }

    info!("snapshot_rankings: {} periods snapshotted", PERIODS.len());
    Ok(())
}

/// Spawn a background task that snapshots rankings every `interval_secs`.
pub fn spawn_snapshot_rankings(pool: PgPool, interval_secs: u64) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
        loop {
            ticker.tick().await;
            if let Err(e) = snapshot_rankings(&pool).await {
                error!("snapshot_rankings failed: {e:#}");
            }
        }
    });
}
