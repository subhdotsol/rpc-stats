use anyhow::Result;
use rpc_cache::{keys, set_json_ex, RedisPool};
use rpc_core::types::db_models::LeaderboardRow;
use sqlx::PgPool;
use tracing::{error, info};

/// Composite score formula:
///   landing_rate * 0.50
///   (1.0 / avg_confirm_ms) * 30000 * 0.30   (normalise ms into 0-1 range roughly)
///   (1.0 / avg_slot_lag)   * 5    * 0.20
pub async fn refresh_leaderboard(pool: &PgPool, redis: &RedisPool) -> Result<()> {
    // 1. Update the durable source of truth (Postgres)
    sqlx::query(
        r#"
        WITH latest AS (
          SELECT
            provider_id,
            landing_rate,
            avg_confirm_ms,
            avg_slot_lag,
            p95_latency_ms,
            avg_claim_vs_reality_ms,
            -- Composite score (higher = better)
            (
              COALESCE(landing_rate, 0) * 0.50
              + (1.0 / NULLIF(avg_confirm_ms, 0)) * 30000.0 * 0.30
              + (1.0 / NULLIF(avg_slot_lag, 0))   * 5.0     * 0.20
            ) AS composite_score
          FROM provider_metrics_5m
          WHERE region_id IS NULL
            AND fee_tier_id IS NULL
            AND time >= NOW() - INTERVAL '10 minutes'
            AND time = (
              SELECT MAX(time)
              FROM provider_metrics_5m pm2
              WHERE pm2.provider_id   = provider_metrics_5m.provider_id
                AND pm2.region_id     IS NULL
                AND pm2.fee_tier_id   IS NULL
            )
        ),
        ranked AS (
          SELECT
            *,
            RANK() OVER (ORDER BY composite_score DESC) AS rank
          FROM latest
        ),
        uptime AS (
          -- 24h uptime = 1 - (minutes with landing_rate < 0.5 / total minutes)
          SELECT
            provider_id,
            1.0 - (
              COUNT(*) FILTER (WHERE landing_rate < 0.50)::DECIMAL
              / NULLIF(COUNT(*), 0)
            ) AS uptime_24h
          FROM provider_metrics_1m
          WHERE region_id   IS NULL
            AND fee_tier_id IS NULL
            AND time >= NOW() - INTERVAL '24 hours'
          GROUP BY provider_id
        )
        INSERT INTO leaderboard_current (
          provider_id, rank, composite_score, landing_rate, avg_confirm_ms,
          avg_slot_lag, p95_latency_ms, avg_claim_vs_reality_ms,
          uptime_24h, status, last_tested_at, updated_at
        )
        SELECT
          r.provider_id,
          r.rank::INT,
          r.composite_score,
          r.landing_rate,
          r.avg_confirm_ms,
          r.avg_slot_lag,
          r.p95_latency_ms,
          r.avg_claim_vs_reality_ms,
          u.uptime_24h,
          CASE
            WHEN r.landing_rate < 0.80   THEN 'outage'
            WHEN r.landing_rate < 0.92   THEN 'degraded'
            WHEN r.p95_latency_ms > 2000 THEN 'degraded'
            ELSE                              'healthy'
          END AS status,
          NOW() AS last_tested_at,
          NOW() AS updated_at
        FROM ranked r
        LEFT JOIN uptime u ON u.provider_id = r.provider_id
        ON CONFLICT (provider_id) DO UPDATE SET
          rank                    = EXCLUDED.rank,
          composite_score         = EXCLUDED.composite_score,
          landing_rate            = EXCLUDED.landing_rate,
          avg_confirm_ms          = EXCLUDED.avg_confirm_ms,
          avg_slot_lag            = EXCLUDED.avg_slot_lag,
          p95_latency_ms          = EXCLUDED.p95_latency_ms,
          avg_claim_vs_reality_ms = EXCLUDED.avg_claim_vs_reality_ms,
          uptime_24h              = EXCLUDED.uptime_24h,
          status                  = EXCLUDED.status,
          last_tested_at          = EXCLUDED.last_tested_at,
          updated_at              = EXCLUDED.updated_at
        "#,
    )
    .execute(pool)
    .await?;

    // 2. Query back the state to push to the hot read path (Redis)
    let rows = sqlx::query_as::<_, LeaderboardRow>(
        "SELECT * FROM leaderboard_current ORDER BY rank ASC"
    )
    .fetch_all(pool)
    .await?;

    set_json_ex(
        redis,
        keys::LEADERBOARD_CURRENT,
        &rows,
        keys::LEADERBOARD_CURRENT_TTL,
    )
    .await?;

    info!("refresh_leaderboard: DB updated and synced to Redis ({} rows)", rows.len());
    Ok(())
}

/// Spawn a background task that refreshes the leaderboard every `interval_secs`.
pub fn spawn_refresh_leaderboard(pool: PgPool, redis: RedisPool, interval_secs: u64) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
        loop {
            ticker.tick().await;
            if let Err(e) = refresh_leaderboard(&pool, &redis).await {
                error!("refresh_leaderboard failed: {e:#}");
            }
        }
    });
}

