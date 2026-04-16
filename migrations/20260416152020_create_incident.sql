-- Add migration script here
CREATE TABLE incidents (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rpc_id        UUID        NOT NULL REFERENCES rpcs (id) ON DELETE CASCADE,
    region        TEXT        NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at   TIMESTAMPTZ,
    reason        TEXT        NOT NULL,
    -- computed: resolved_at - started_at
    duration_ms   BIGINT GENERATED ALWAYS AS (
        CASE
            WHEN resolved_at IS NOT NULL
            THEN EXTRACT(EPOCH FROM (resolved_at - started_at))::BIGINT * 1000
            ELSE NULL
        END
    ) STORED
);
 
CREATE INDEX idx_incidents_rpc_id     ON incidents (rpc_id);
CREATE INDEX idx_incidents_region     ON incidents (region);
CREATE INDEX idx_incidents_open       ON incidents (rpc_id, region) WHERE resolved_at IS NULL;
CREATE INDEX idx_incidents_started_at ON incidents (started_at DESC);
 