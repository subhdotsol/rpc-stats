use chrono::{DateTime, Timelike, Utc};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::warn;

use rpc_core::types::{TxConfirmed, TxLanded, TxSubmitted, TxTimeout};

/// Tracks the lifecycle of a single probe transaction across Kafka events.
/// Inserted on the first event seen (any type) and enriched as subsequent
/// events arrive out-of-order.
#[derive(Debug, Clone)]
pub struct TxState {
    // ── From tx.submitted ────────────────────────────────────────────────
    pub provider_id: String,
    pub region_id: Option<String>,
    pub fee_tier_id: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub submitted_slot: Option<i64>,
    pub network_tps: Option<i32>,

    // ── From tx.landed ───────────────────────────────────────────────────
    pub landed_slot: Option<i64>,
    pub geyser_landed_at: Option<DateTime<Utc>>,

    // ── From tx.confirmed ────────────────────────────────────────────────
    pub rpc_confirmed_at: Option<DateTime<Utc>>,

    // ── From tx.timeout ──────────────────────────────────────────────────
    pub timed_out: bool,

    // ── Housekeeping ─────────────────────────────────────────────────────
    /// Wall-clock instant when first inserted (for TTL-based eviction).
    pub inserted_at: Instant,
    /// Minute-truncated submitted_at — determines which 60s window this
    /// transaction belongs to. None until tx.submitted arrives.
    pub window_key: Option<DateTime<Utc>>,
}

impl TxState {
    fn empty(provider_id: String) -> Self {
        Self {
            provider_id,
            region_id: None,
            fee_tier_id: None,
            submitted_at: None,
            submitted_slot: None,
            network_tps: None,
            landed_slot: None,
            geyser_landed_at: None,
            rpc_confirmed_at: None,
            timed_out: false,
            inserted_at: Instant::now(),
            window_key: None,
        }
    }
}

pub type TxStore = Arc<DashMap<String, TxState>>;

pub fn new_store() -> TxStore {
    Arc::new(DashMap::with_capacity(4096))
}

/// Truncate a DateTime to the start of its minute (zero out seconds/nanos).
fn truncate_to_minute(dt: DateTime<Utc>) -> DateTime<Utc> {
    dt.with_second(0)
        .and_then(|d| d.with_nanosecond(0))
        .unwrap_or(dt)
}

// ── Per-event handlers ───────────────────────────────────────────────────────

pub fn handle_submitted(store: &TxStore, event: TxSubmitted) {
    let window_key = truncate_to_minute(event.submitted_at);

    store
        .entry(event.signature.clone())
        .and_modify(|s| {
            s.provider_id = event.provider_id.clone();
            s.region_id = Some(event.region_id.clone());
            s.fee_tier_id = Some(event.fee_tier_id.clone());
            s.submitted_at = Some(event.submitted_at);
            s.submitted_slot = event.submitted_slot;
            s.network_tps = event.network_tps;
            s.window_key = Some(window_key);
        })
        .or_insert_with(|| {
            let mut s = TxState::empty(event.provider_id);
            s.region_id = Some(event.region_id);
            s.fee_tier_id = Some(event.fee_tier_id);
            s.submitted_at = Some(event.submitted_at);
            s.submitted_slot = event.submitted_slot;
            s.network_tps = event.network_tps;
            s.window_key = Some(window_key);
            s
        });
}

pub fn handle_landed(store: &TxStore, event: TxLanded) {
    store
        .entry(event.signature.clone())
        .and_modify(|s| {
            s.landed_slot = Some(event.landed_slot);
            s.geyser_landed_at = Some(event.geyser_landed_at);
        })
        .or_insert_with(|| {
            warn!(sig = %event.signature, "tx.landed arrived before tx.submitted");
            let mut s = TxState::empty(event.provider_id);
            s.landed_slot = Some(event.landed_slot);
            s.geyser_landed_at = Some(event.geyser_landed_at);
            s
        });
}

pub fn handle_confirmed(store: &TxStore, event: TxConfirmed) {
    store
        .entry(event.signature.clone())
        .and_modify(|s| {
            s.rpc_confirmed_at = Some(event.rpc_confirmed_at);
        })
        .or_insert_with(|| {
            warn!(sig = %event.signature, "tx.confirmed arrived before tx.submitted");
            let mut s = TxState::empty(event.provider_id);
            s.rpc_confirmed_at = Some(event.rpc_confirmed_at);
            s
        });
}

pub fn handle_timeout(store: &TxStore, event: TxTimeout) {
    store
        .entry(event.signature.clone())
        .and_modify(|s| {
            s.timed_out = true;
        })
        .or_insert_with(|| {
            warn!(sig = %event.signature, "tx.timeout arrived before tx.submitted");
            let mut s = TxState::empty(event.provider_id);
            s.timed_out = true;
            s
        });
}
