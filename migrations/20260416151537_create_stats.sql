-- Add migration script here
-- This is the hot append-only table.
-- Partition by region for query performance.
-- If moving to TimescaleDB later: SELECT create_hypertable('state', 'checked_at');
 
CREATE TABLE state (
    id             UUID        NOT NULL DEFAULT uuid_generate_v4(),
    rpc_id         UUID        NOT NULL REFERENCES rpcs (id) ON DELETE CASCADE,
    region         TEXT        NOT NULL,
    checked_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checked_by     UUID        REFERENCES workers (id) ON DELETE SET NULL,
    latency_ms     INT,
    block_number   BIGINT,
    block_lag      INT,
    is_healthy     BOOLEAN     NOT NULL DEFAULT FALSE,
    error_code     TEXT,
    error_message  TEXT,
 
    PRIMARY KEY (id, checked_at)
);
-- PARTITION BY RANGE (checked_at);
 
-- -- Create monthly partitions (add more as needed / automate with pg_partman)
-- CREATE TABLE state_2026_04 PARTITION OF state
--     FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
 
-- CREATE TABLE state_2026_05 PARTITION OF state
--     FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
 
-- CREATE TABLE state_2026_06 PARTITION OF state
--     FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
 
-- Indexes on each partition are inherited
CREATE INDEX idx_state_rpc_region_time ON state (rpc_id, region, checked_at DESC);
CREATE INDEX idx_state_is_healthy      ON state (is_healthy, checked_at DESC);
CREATE INDEX idx_state_checked_by      ON state (checked_by);
 