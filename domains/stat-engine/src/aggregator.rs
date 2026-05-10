use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::{DateTime, Timelike, Utc};
use rpc_cache::RedisPool;
use serde::Serialize;
use sqlx::PgPool;
use tracing::{error, info, warn};

use crate::metrics::{compute_window_metrics, WindowMetrics};
use crate::state::{TxState, TxStore};

/// A row ready to be inserted into `provider_metrics_1m`.
struct MetricsRow {
    time: DateTime<Utc>,
    provider_id: String,
    region_id: Option<String>,
    fee_tier_id: Option<String>,
    metrics: WindowMetrics,
}

/// Per-provider summary pushed to Redis for real-time reads.
#[derive(Serialize)]
struct ProviderLatestMetrics {
    time: DateTime<Utc>,
    landing_rate: Option<f64>,
    p50_latency_ms: Option<i32>,
    p95_latency_ms: Option<i32>,
    avg_confirm_ms: Option<i32>,
    avg_slot_lag: Option<f64>,
    tx_submitted: i32,
    tx_landed: i32,
}

/// Spawn the 60-second flush task that drains the in-memory store,
/// computes aggregated metrics, and writes to Postgres + Redis.
pub fn spawn_flush_task(store: TxStore, pool: PgPool, redis: RedisPool) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(60));
        // Skip the immediate first tick
        ticker.tick().await;

        loop {
            ticker.tick().await;
            if let Err(e) = flush_window(&store, &pool, &redis).await {
                error!("flush_window failed: {e:#}");
            }
        }
    });
}

/// Spawn a 5-minute safety sweep that evicts any entry older than 5 minutes.
pub fn spawn_cleanup_task(store: TxStore) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(300));
        ticker.tick().await;

        loop {
            ticker.tick().await;
            let cutoff = Instant::now() - Duration::from_secs(300);
            let before = store.len();
            store.retain(|_sig, state| state.inserted_at > cutoff);
            let evicted = before - store.len();
            if evicted > 0 {
                info!(evicted, remaining = store.len(), "stale entry sweep");
            }
        }
    });
}

/// Drain completed entries, compute metrics, write to PG + Redis.
async fn flush_window(store: &TxStore, pool: &PgPool, redis: &RedisPool) -> Result<()> {
    let now = Utc::now();
    let window_end = truncate_to_minute(now);

    // ── 1. Drain entries whose window has closed ─────────────────────────────
    let orphan_cutoff = Instant::now() - Duration::from_secs(90);
    let mut drained: Vec<(String, TxState)> = Vec::new();

    store.retain(|_sig, state| {
        // Drain if window is complete
        if let Some(wk) = state.window_key {
            if wk < window_end {
                drained.push((_sig.clone(), state.clone()));
                return false; // remove from store
            }
        }
        // Drain orphans (no submitted_at after 90s)
        if state.window_key.is_none() && state.inserted_at < orphan_cutoff {
            return false; // evict
        }
        true // keep
    });

    if drained.is_empty() {
        return Ok(());
    }

    // ── 2. Group by (provider, region, fee_tier) ─────────────────────────────
    // Also track per-provider entries for rollup rows
    let mut specific_groups: HashMap<(String, String, String), Vec<TxState>> = HashMap::new();
    let mut per_provider: HashMap<String, Vec<TxState>> = HashMap::new();
    let mut per_provider_fee: HashMap<(String, String), Vec<TxState>> = HashMap::new();

    for (_sig, state) in &drained {
        let provider = state.provider_id.clone();
        let region = match &state.region_id {
            Some(r) => r.clone(),
            None => continue, // skip entries without region (orphans)
        };
        let fee_tier = match &state.fee_tier_id {
            Some(f) => f.clone(),
            None => continue, // skip entries without fee_tier (orphans)
        };

        specific_groups
            .entry((provider.clone(), region, fee_tier.clone()))
            .or_default()
            .push(state.clone());

        per_provider_fee
            .entry((provider.clone(), fee_tier))
            .or_default()
            .push(state.clone());

        per_provider
            .entry(provider)
            .or_default()
            .push(state.clone());
    }

    // ── 3. Compute metrics and build rows ────────────────────────────────────
    let mut rows: Vec<MetricsRow> = Vec::new();

    // Determine window time from the drained entries
    let window_time = drained
        .iter()
        .filter_map(|(_, s)| s.window_key)
        .min()
        .unwrap_or(window_end);

    // Row type 1: (provider, region, fee_tier) — drilled down
    for ((provider, region, fee_tier), entries) in &specific_groups {
        let metrics = compute_window_metrics(entries);
        if metrics.tx_submitted == 0 {
            continue;
        }
        rows.push(MetricsRow {
            time: window_time,
            provider_id: provider.clone(),
            region_id: Some(region.clone()),
            fee_tier_id: Some(fee_tier.clone()),
            metrics,
        });
    }

    // Row type 2: (provider, NULL, fee_tier) — all-region per fee tier
    for ((provider, fee_tier), entries) in &per_provider_fee {
        let metrics = compute_window_metrics(entries);
        if metrics.tx_submitted == 0 {
            continue;
        }
        rows.push(MetricsRow {
            time: window_time,
            provider_id: provider.clone(),
            region_id: None,
            fee_tier_id: Some(fee_tier.clone()),
            metrics,
        });
    }

    // Row type 3: (provider, NULL, NULL) — fully aggregated (leaderboard row)
    for (provider, entries) in &per_provider {
        let metrics = compute_window_metrics(entries);
        if metrics.tx_submitted == 0 {
            continue;
        }
        rows.push(MetricsRow {
            time: window_time,
            provider_id: provider.clone(),
            region_id: None,
            fee_tier_id: None,
            metrics,
        });
    }

    if rows.is_empty() {
        return Ok(());
    }

    // ── 4. Write to Postgres (DELETE + INSERT in transaction) ─────────────────
    let providers_in_window: Vec<String> = per_provider.keys().cloned().collect();
    write_to_postgres(pool, window_time, &providers_in_window, &rows).await?;

    // ── 5. Push per-provider summary to Redis ────────────────────────────────
    push_to_redis(redis, window_time, &per_provider).await;

    info!(
        window = %window_time,
        drained = drained.len(),
        rows_written = rows.len(),
        providers = providers_in_window.len(),
        remaining = store.len(),
        "flush complete"
    );

    Ok(())
}

async fn write_to_postgres(
    pool: &PgPool,
    window_time: DateTime<Utc>,
    providers: &[String],
    rows: &[MetricsRow],
) -> Result<()> {
    let mut tx = pool.begin().await?;

    // Delete existing rows for this window for all affected providers
    for provider_id in providers {
        sqlx::query(
            "DELETE FROM provider_metrics_1m WHERE time = $1 AND provider_id = $2",
        )
        .bind(window_time)
        .bind(provider_id)
        .execute(&mut *tx)
        .await?;
    }

    // Insert all rows
    for row in rows {
        let m = &row.metrics;
        sqlx::query(
            r#"
            INSERT INTO provider_metrics_1m (
                time, provider_id, region_id, fee_tier_id,
                tx_submitted, tx_landed, tx_dropped, tx_timeout,
                landing_rate, p50_latency_ms, p95_latency_ms, p99_latency_ms,
                avg_confirm_ms, avg_slot_lag, avg_claim_vs_reality_ms, avg_network_tps
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            "#,
        )
        .bind(row.time)
        .bind(&row.provider_id)
        .bind(&row.region_id)
        .bind(&row.fee_tier_id)
        .bind(m.tx_submitted)
        .bind(m.tx_landed)
        .bind(m.tx_dropped)
        .bind(m.tx_timeout)
        .bind(m.landing_rate)
        .bind(m.p50_latency_ms)
        .bind(m.p95_latency_ms)
        .bind(m.p99_latency_ms)
        .bind(m.avg_confirm_ms)
        .bind(m.avg_slot_lag)
        .bind(m.avg_claim_vs_reality_ms)
        .bind(m.avg_network_tps)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn push_to_redis(
    redis: &RedisPool,
    window_time: DateTime<Utc>,
    per_provider: &HashMap<String, Vec<TxState>>,
) {
    for (provider_id, entries) in per_provider {
        let metrics = compute_window_metrics(entries);
        if metrics.tx_submitted == 0 {
            continue;
        }

        let summary = ProviderLatestMetrics {
            time: window_time,
            landing_rate: metrics.landing_rate,
            p50_latency_ms: metrics.p50_latency_ms,
            p95_latency_ms: metrics.p95_latency_ms,
            avg_confirm_ms: metrics.avg_confirm_ms,
            avg_slot_lag: metrics.avg_slot_lag,
            tx_submitted: metrics.tx_submitted,
            tx_landed: metrics.tx_landed,
        };

        let key = rpc_cache::keys::provider_metrics_latest(provider_id);
        if let Err(e) =
            rpc_cache::set_json_ex(redis, &key, &summary, rpc_cache::keys::PROVIDER_METRICS_LATEST_TTL).await
        {
            warn!(provider = %provider_id, "Redis push failed: {e}");
        }
    }
}

fn truncate_to_minute(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_second(0)
        .and_then(|d| d.with_nanosecond(0))
        .unwrap_or(dt)
}
