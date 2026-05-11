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
        p95_sub AS (
          -- p95 computed from 1-minute base table (not in the 5m aggregate)
          SELECT
            provider_id,
            PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY p95_latency_ms) AS p95_latency_ms
          FROM provider_metrics_1m
          WHERE region_id IS NULL
            AND fee_tier_id IS NULL
            AND time >= NOW() - INTERVAL '10 minutes'
            AND p95_latency_ms IS NOT NULL
          GROUP BY provider_id
        ),
        ranked AS (
          SELECT
            l.*,
            p.p95_latency_ms,
            RANK() OVER (ORDER BY l.composite_score DESC) AS rank
          FROM latest l
          LEFT JOIN p95_sub p ON p.provider_id = l.provider_id
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
            WHEN r.landing_rate < 0.80                          THEN 'outage'
            WHEN r.landing_rate < 0.92                          THEN 'degraded'
            WHEN COALESCE(r.p95_latency_ms, 0) > 2000          THEN 'degraded'
            ELSE                                                     'healthy'
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

    // 2. Query back the state
    let rows = sqlx::query_as::<_, LeaderboardRow>(
        "SELECT * FROM leaderboard_current ORDER BY rank ASC"
    )
    .fetch_all(pool)
    .await?;

    #[derive(sqlx::FromRow)]
    struct TrendRow {
        provider_id: String,
        landing_rate: Option<f64>,
    }

    // 3. Query trend data (last 24 hours from 1h buckets)
    let trend_rows = sqlx::query_as::<_, TrendRow>(
        r#"
        SELECT provider_id, landing_rate::FLOAT
        FROM provider_metrics_1h
        WHERE time >= NOW() - INTERVAL '24 hours'
          AND region_id IS NULL AND fee_tier_id IS NULL
        ORDER BY provider_id, time ASC
        "#
    )
    .fetch_all(pool)
    .await?;

    use std::collections::HashMap;
    let mut trends_map: HashMap<String, Vec<f64>> = HashMap::new();
    for tr in trend_rows {
        let val = tr.landing_rate.unwrap_or(0.0) * 100.0;
        trends_map.entry(tr.provider_id).or_default().push(val);
    }

    use rpc_core::types::api::LeaderboardEntry;

    let mut entries = Vec::new();
    for r in rows {
        let t_data = trends_map.get(&r.provider_id).cloned().unwrap_or_default();
        let mut trend = "stable".to_string();
        if t_data.len() >= 2 {
            let mid = t_data.len() / 2;
            let first_half_avg: f64 = t_data[..mid].iter().sum::<f64>() / (mid as f64);
            let second_half_avg: f64 = t_data[mid..].iter().sum::<f64>() / ((t_data.len() - mid) as f64);
            if second_half_avg > first_half_avg + 0.5 {
                trend = "rising".to_string();
            } else if second_half_avg < first_half_avg - 0.5 {
                trend = "declining".to_string();
            }
        }

        entries.push(LeaderboardEntry {
            provider: r.provider_id,
            rank: r.rank,
            landing_rate: r.landing_rate.unwrap_or(0.0) * 100.0,
            avg_confirm: r.avg_confirm_ms.unwrap_or(0),
            slot_lag: r.avg_slot_lag.unwrap_or(0.0),
            p95_latency: r.p95_latency_ms.unwrap_or(0),
            claim_vs_reality: r.avg_claim_vs_reality_ms.unwrap_or(0),
            uptime24h: r.uptime_24h.unwrap_or(0.0) * 100.0,
            status: r.status.unwrap_or_else(|| "outage".to_string()),
            trend,
            trend_data: t_data,
        });
    }

    set_json_ex(
        redis,
        keys::LEADERBOARD_CURRENT,
        &entries,
        keys::LEADERBOARD_CURRENT_TTL,
    )
    .await?;

    info!("refresh_leaderboard: DB updated and synced to Redis ({} rows)", entries.len());
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

