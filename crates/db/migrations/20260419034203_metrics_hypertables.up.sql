-- Add up migration script here

-- Requires TimescaleDB extension. Run `CREATE EXTENSION IF NOT EXISTS timescaledb;`
-- on your Postgres instance before applying this migration (superuser required,
-- do it once manually or in your DB init script — TimescaleDB must be in
-- shared_preload_libraries before it can be created as an extension).

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ── Base 1-minute metrics table ───────────────────────────────────────────────
-- Written by the Kafka Streams stat engine at the end of every 60-second window.
--
-- Row strategy (3 row types per provider per minute):
--   1. region_id = <specific>, fee_tier_id = <specific>  → raw drilled-down data
--   2. region_id = NULL,       fee_tier_id = <specific>  → all-region per fee tier
--   3. region_id = NULL,       fee_tier_id = NULL        → fully aggregated row
--                                                           (used by leaderboard)
--
-- The NULL sentinel approach avoids expensive GROUP BY rollups at read time.

CREATE TABLE provider_metrics_1m (
    time                    TIMESTAMPTZ   NOT NULL,
    provider_id             VARCHAR(50)   NOT NULL,
    region_id               VARCHAR(50),              -- NULL = all regions combined
    fee_tier_id             VARCHAR(50),              -- NULL = all fee tiers combined

    -- Volume counters for this 1-minute bucket
    tx_submitted            INT           NOT NULL DEFAULT 0,
    tx_landed               INT           NOT NULL DEFAULT 0,
    tx_dropped              INT           NOT NULL DEFAULT 0,
    tx_timeout              INT           NOT NULL DEFAULT 0,

    -- landing_rate = tx_landed / tx_submitted (pre-computed by stat engine)
    landing_rate            DECIMAL(5, 4),

    -- Latency percentiles in milliseconds (computed from raw landing_time_ms values
    -- using percentile_disc in the Kafka Streams aggregation window)
    p50_latency_ms          INT,
    p95_latency_ms          INT,
    p99_latency_ms          INT,
    avg_confirm_ms          INT,

    -- Average slot lag (slots behind chain tip) across all txns in this window
    avg_slot_lag            DECIMAL(10, 2),

    -- Average of claim_vs_reality_ms across landed txns in this window
    avg_claim_vs_reality_ms INT,

    -- Network context for congestion correlation
    avg_network_tps         INT,

    UNIQUE (time, provider_id, region_id, fee_tier_id)
);

SELECT create_hypertable(
    'provider_metrics_1m',
    'time',
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX ON provider_metrics_1m (provider_id, time DESC);
CREATE INDEX ON provider_metrics_1m (provider_id, region_id, time DESC);
CREATE INDEX ON provider_metrics_1m (provider_id, fee_tier_id, time DESC);
-- Partial index for the leaderboard's most common query pattern
CREATE INDEX ON provider_metrics_1m (provider_id, time DESC)
    WHERE region_id IS NULL AND fee_tier_id IS NULL;

-- ── Continuous aggregate: 5 minutes ──────────────────────────────────────────
-- Auto-computed by TimescaleDB from provider_metrics_1m.
-- Used by: leaderboard refresh, 24h trend sparklines, incident detector.

CREATE MATERIALIZED VIEW provider_metrics_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time)                                        AS time,
    provider_id,
    region_id,
    fee_tier_id,
    SUM(tx_submitted)                                                     AS tx_submitted,
    SUM(tx_landed)                                                        AS tx_landed,
    SUM(tx_dropped)                                                       AS tx_dropped,
    SUM(tx_timeout)                                                       AS tx_timeout,
    SUM(tx_landed)::DECIMAL / NULLIF(SUM(tx_submitted), 0)               AS landing_rate,
    ROUND(AVG(avg_confirm_ms))::INT                                       AS avg_confirm_ms,
    ROUND(AVG(avg_slot_lag), 2)                                           AS avg_slot_lag,
    ROUND(AVG(avg_claim_vs_reality_ms))::INT                              AS avg_claim_vs_reality_ms,
    ROUND(AVG(avg_network_tps))::INT                                      AS avg_network_tps
FROM provider_metrics_1m
GROUP BY
    time_bucket('5 minutes', time),
    provider_id,
    region_id,
    fee_tier_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'provider_metrics_5m',
    start_offset      => INTERVAL '1 hour',
    end_offset        => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes'
);




-- ── Continuous aggregate: 1 hour (built from 5m) ─────────────────────────────
-- Used by: 7-day trend charts, rank snapshot cron.

CREATE MATERIALIZED VIEW provider_metrics_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time)                                           AS time,
    provider_id,
    region_id,
    fee_tier_id,
    SUM(tx_submitted)                                                     AS tx_submitted,
    SUM(tx_landed)                                                        AS tx_landed,
    SUM(tx_dropped)                                                       AS tx_dropped,
    SUM(tx_timeout)                                                       AS tx_timeout,
    SUM(tx_landed)::DECIMAL / NULLIF(SUM(tx_submitted), 0)               AS landing_rate,
    ROUND(AVG(avg_confirm_ms))::INT                                       AS avg_confirm_ms,
    ROUND(AVG(avg_slot_lag), 2)                                           AS avg_slot_lag,
    ROUND(AVG(avg_claim_vs_reality_ms))::INT                              AS avg_claim_vs_reality_ms
FROM provider_metrics_5m
GROUP BY
    time_bucket('1 hour', time),
    provider_id,
    region_id,
    fee_tier_id
WITH NO DATA;


SELECT add_continuous_aggregate_policy(
    'provider_metrics_1h',
    start_offset      => INTERVAL '12 hours',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

-- ── Continuous aggregate: 1 day (built from 1h) ───────────────────────────────
-- Used by: 30-day and 90-day trend charts.

CREATE MATERIALIZED VIEW provider_metrics_1d
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time)                                            AS time,
    provider_id,
    region_id,
    fee_tier_id,
    SUM(tx_submitted)                                                     AS tx_submitted,
    SUM(tx_landed)                                                        AS tx_landed,
    SUM(tx_dropped)                                                       AS tx_dropped,
    SUM(tx_timeout)                                                       AS tx_timeout,
    SUM(tx_landed)::DECIMAL / NULLIF(SUM(tx_submitted), 0)               AS landing_rate,
    ROUND(AVG(avg_confirm_ms))::INT                                       AS avg_confirm_ms,
    ROUND(AVG(avg_slot_lag), 2)                                           AS avg_slot_lag,
    ROUND(AVG(avg_claim_vs_reality_ms))::INT                              AS avg_claim_vs_reality_ms
FROM provider_metrics_1h
GROUP BY
    time_bucket('1 day', time),
    provider_id,
    region_id,
    fee_tier_id
WITH NO DATA;


SELECT add_continuous_aggregate_policy(
    'provider_metrics_1d',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '6 hours'
);