/// Typed Redis key constants and TTL values matching the stat engine keymap.
///
/// Static keys are plain `&str` constants.
/// Per-provider keys are free functions returning `String`.

// LEADERBOARD
pub const LEADERBOARD_CURRENT: &str = "leaderboard:current";
pub const LEADERBOARD_CURRENT_TTL: u64 = 35;

// PER-PROVIDER
pub fn provider_fee_breakdown(id: &str) -> String {
    format!("provider:{id}:fee-breakdown")
}
pub const PROVIDER_FEE_BREAKDOWN_TTL: u64 = 35;

pub fn provider_region_latency(id: &str) -> String {
    format!("provider:{id}:region-latency")
}
pub const PROVIDER_REGION_LATENCY_TTL: u64 = 35;

pub fn provider_trend_24h(id: &str) -> String {
    format!("provider:{id}:trend:24h")
}
pub const PROVIDER_TREND_24H_TTL: u64 = 120;

pub fn provider_trend_7d(id: &str) -> String {
    format!("provider:{id}:trend:7d")
}
pub const PROVIDER_TREND_7D_TTL: u64 = 300;

// TEST RUNS
pub const TEST_RUNS_LATEST: &str = "test-runs:latest";
pub const TEST_RUNS_LATEST_TTL: u64 = 10;

// BENCHMARKS
pub const BENCHMARKS_RPC_METHODS: &str = "benchmarks:rpc-methods";
pub const BENCHMARKS_RPC_METHODS_TTL: u64 = 120;

// INCIDENTS
pub const INCIDENTS_ACTIVE: &str = "incidents:active";
pub const INCIDENTS_ACTIVE_TTL: u64 = 30;

// NETWORK
pub const NETWORK_CURRENT: &str = "network:current";
pub const NETWORK_CURRENT_TTL: u64 = 5;
