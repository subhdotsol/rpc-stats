-- Add up migration script here

-- ── Leaderboard current state ─────────────────────────────────────────────────
-- Pre-computed snapshot refreshed every 30 seconds by the stat engine.
-- Redis is the primary read path (sub-millisecond); this table is the
-- durable source of truth and fallback when Redis is cold.

CREATE TABLE leaderboard_current (
    provider_id              VARCHAR(50)   PRIMARY KEY REFERENCES providers(id),
    rank                     INT           NOT NULL,

    -- Composite score formula (tunable weights):
    --   (landing_rate * 0.50)
    --   + (1.0 / avg_confirm_ms) * 30000.0 * 0.30
    --   + (1.0 / avg_slot_lag)   * 5.0      * 0.20
    composite_score          DECIMAL(8, 4) NOT NULL DEFAULT 0,

    landing_rate             DECIMAL(5, 4),          -- 0.0000–1.0000
    avg_confirm_ms           INT,
    avg_slot_lag             DECIMAL(10, 2),
    p95_latency_ms           INT,
    avg_claim_vs_reality_ms  INT,

    -- uptime_24h: fraction of 1-minute buckets in last 24h where landing_rate >= 0.50
    uptime_24h               DECIMAL(5, 4),

    -- 'healthy' | 'degraded' | 'outage'
    -- Thresholds: outage < 0.80, degraded < 0.92 OR p95 > 2000ms
    status                   VARCHAR(20),

    last_tested_at           TIMESTAMPTZ,
    updated_at               TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- ── Incidents ─────────────────────────────────────────────────────────────────
-- Auto-detected by the incident detector service (runs every 30s).
-- Also manually insertable for planned maintenance entries.

CREATE TABLE incidents (
    id               BIGSERIAL    PRIMARY KEY,
    provider_id      VARCHAR(50)  NOT NULL REFERENCES providers(id),

    -- 'outage'      → landing_rate < 0.80
    -- 'degraded'    → landing_rate < 0.92 OR p95_latency > 2000ms
    -- 'maintenance' → manually inserted for planned downtime
    incident_type    VARCHAR(20)  NOT NULL CHECK (incident_type IN ('outage', 'degraded', 'maintenance')),

    started_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved_at      TIMESTAMPTZ,

    -- Populated on resolve by the auto-resolve trigger.
    duration_seconds INT,

    description      TEXT,

    -- Which metric breached the threshold (for display on the incidents tab).
    trigger_metric   VARCHAR(50),       -- 'landing_rate' | 'p95_latency' | 'slot_lag'
    trigger_value    DECIMAL(10, 4),    -- The breaching value, e.g. 0.7812 or 2341

    is_resolved      BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_incidents_provider_time ON incidents (provider_id, started_at DESC);
CREATE INDEX idx_incidents_active        ON incidents (is_resolved) WHERE is_resolved = FALSE;
CREATE INDEX idx_incidents_recent        ON incidents (started_at DESC);
-- Partial index so the detector's "is there already an open incident?" check is O(1)
CREATE INDEX idx_incidents_open_provider ON incidents (provider_id, incident_type)
    WHERE is_resolved = FALSE;

-- ── Rank snapshots ────────────────────────────────────────────────────────────
-- Written by a daily cron job for each historical period.
-- Powering the "Ranking History" tab on the Performance Trends page.

CREATE TABLE rank_snapshots (
    id              BIGSERIAL    PRIMARY KEY,
    snapshot_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    snapshot_date   DATE         NOT NULL DEFAULT CURRENT_DATE,
    -- 'today' | '1d' | '3d' | '7d' | '14d' | '30d' | '90d'
    period          VARCHAR(20)  NOT NULL,
    provider_id     VARCHAR(50)  NOT NULL REFERENCES providers(id),
    rank            INT          NOT NULL,
    composite_score DECIMAL(8, 4),
    landing_rate    DECIMAL(5, 4),
    avg_confirm_ms  INT,
    avg_slot_lag    DECIMAL(10, 2)
);


CREATE INDEX idx_rank_snapshots_period ON rank_snapshots (period, snapshot_at DESC);
-- Prevent duplicate snapshots for the same provider+period on the same calendar day
CREATE UNIQUE INDEX idx_rank_snapshots_unique
    ON rank_snapshots (period, provider_id, snapshot_date);