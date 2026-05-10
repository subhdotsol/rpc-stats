use crate::state::TxState;

/// Aggregated metrics for one (provider, region, fee_tier) group in a 60s window.
#[derive(Debug, Clone)]
pub struct WindowMetrics {
    pub tx_submitted: i32,
    pub tx_landed: i32,
    pub tx_dropped: i32,
    pub tx_timeout: i32,
    pub landing_rate: Option<f64>,
    pub p50_latency_ms: Option<i32>,
    pub p95_latency_ms: Option<i32>,
    pub p99_latency_ms: Option<i32>,
    pub avg_confirm_ms: Option<i32>,
    pub avg_slot_lag: Option<f64>,
    pub avg_claim_vs_reality_ms: Option<i32>,
    pub avg_network_tps: Option<i32>,
}

/// Compute the value at the given percentile from a sorted slice.
/// Uses the nearest-rank method.
fn percentile(sorted: &[i64], p: f64) -> Option<i32> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    Some(sorted[idx] as i32)
}

/// Compute all window metrics from a slice of TxState entries that share the
/// same (provider, region, fee_tier) group.
pub fn compute_window_metrics(entries: &[TxState]) -> WindowMetrics {
    let tx_submitted = entries
        .iter()
        .filter(|e| e.submitted_at.is_some())
        .count() as i32;

    let tx_landed = entries
        .iter()
        .filter(|e| e.geyser_landed_at.is_some())
        .count() as i32;

    let tx_timeout = entries.iter().filter(|e| e.timed_out).count() as i32;

    let tx_dropped = (tx_submitted - tx_landed - tx_timeout).max(0);

    let landing_rate = if tx_submitted > 0 {
        Some(tx_landed as f64 / tx_submitted as f64)
    } else {
        None
    };

    // ── Latency percentiles (from landing_time_ms = geyser_landed_at - submitted_at) ──
    let mut latencies: Vec<i64> = entries
        .iter()
        .filter_map(|e| {
            let submitted = e.submitted_at?;
            let landed = e.geyser_landed_at?;
            Some((landed - submitted).num_milliseconds())
        })
        .filter(|ms| *ms >= 0)
        .collect();
    latencies.sort_unstable();

    let p50_latency_ms = percentile(&latencies, 50.0);
    let p95_latency_ms = percentile(&latencies, 95.0);
    let p99_latency_ms = percentile(&latencies, 99.0);

    // ── avg_confirm_ms (rpc_confirmed_at - submitted_at) ──
    let confirm_times: Vec<i64> = entries
        .iter()
        .filter_map(|e| {
            let submitted = e.submitted_at?;
            let confirmed = e.rpc_confirmed_at?;
            let ms = (confirmed - submitted).num_milliseconds();
            if ms >= 0 { Some(ms) } else { None }
        })
        .collect();

    let avg_confirm_ms = if confirm_times.is_empty() {
        None
    } else {
        Some((confirm_times.iter().sum::<i64>() / confirm_times.len() as i64) as i32)
    };

    // ── avg_slot_lag (landed_slot - submitted_slot) ──
    let slot_lags: Vec<f64> = entries
        .iter()
        .filter_map(|e| {
            let submitted_slot = e.submitted_slot?;
            let landed_slot = e.landed_slot?;
            Some((landed_slot - submitted_slot) as f64)
        })
        .filter(|lag| *lag >= 0.0)
        .collect();

    let avg_slot_lag = if slot_lags.is_empty() {
        None
    } else {
        Some(slot_lags.iter().sum::<f64>() / slot_lags.len() as f64)
    };

    // ── avg_claim_vs_reality_ms (confirm_ms - landing_ms) ──
    let claim_vs_reality: Vec<i64> = entries
        .iter()
        .filter_map(|e| {
            let submitted = e.submitted_at?;
            let landed = e.geyser_landed_at?;
            let confirmed = e.rpc_confirmed_at?;
            let landing_ms = (landed - submitted).num_milliseconds();
            let confirm_ms = (confirmed - submitted).num_milliseconds();
            Some(confirm_ms - landing_ms)
        })
        .collect();

    let avg_claim_vs_reality_ms = if claim_vs_reality.is_empty() {
        None
    } else {
        Some((claim_vs_reality.iter().sum::<i64>() / claim_vs_reality.len() as i64) as i32)
    };

    // ── avg_network_tps ──
    let tps_values: Vec<i32> = entries.iter().filter_map(|e| e.network_tps).collect();

    let avg_network_tps = if tps_values.is_empty() {
        None
    } else {
        Some(tps_values.iter().map(|v| *v as i64).sum::<i64>() as i32 / tps_values.len() as i32)
    };

    WindowMetrics {
        tx_submitted,
        tx_landed,
        tx_dropped,
        tx_timeout,
        landing_rate,
        p50_latency_ms,
        p95_latency_ms,
        p99_latency_ms,
        avg_confirm_ms,
        avg_slot_lag,
        avg_claim_vs_reality_ms,
        avg_network_tps,
    }
}
