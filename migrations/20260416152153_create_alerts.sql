-- Add migration script here
CREATE TYPE alert_event_type AS ENUM ('down', 'recovered', 'high_latency', 'block_lag');

CREATE TABLE alerts (
    id            UUID             PRIMARY KEY DEFAULT uuid_generate_v4(),
    rpc_id        UUID             NOT NULL REFERENCES rpcs (id) ON DELETE CASCADE,
    url           TEXT             NOT NULL,
    event_type    alert_event_type NOT NULL,
    is_active     BOOLEAN          NOT NULL DEFAULT TRUE,
    -- optional threshold overrides per alert
    latency_threshold_ms  INT,
    block_lag_threshold   INT,
    created_at    TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alerts_rpc_id     ON alerts (rpc_id);
CREATE INDEX idx_alerts_event_type ON alerts (event_type);
CREATE INDEX idx_alerts_is_active  ON alerts (is_active);

CREATE TRIGGER alerts_updated_at
    BEFORE UPDATE ON alerts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();