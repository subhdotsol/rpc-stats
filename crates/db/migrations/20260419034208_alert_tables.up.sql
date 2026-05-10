-- Alert channels: webhook destinations for incident notifications
CREATE TABLE alert_channels (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    channel_type VARCHAR(20) NOT NULL CHECK (channel_type IN ('discord', 'slack', 'generic')),
    webhook_url TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Alert log: history of every alert dispatched (dedup + audit)
CREATE TABLE alert_log (
    id BIGSERIAL PRIMARY KEY,
    incident_id BIGINT NOT NULL REFERENCES incidents(id),
    channel_id BIGINT NOT NULL REFERENCES alert_channels(id) ON DELETE CASCADE,
    event_type VARCHAR(20) NOT NULL CHECK (event_type IN ('created', 'resolved')),
    status VARCHAR(20) NOT NULL DEFAULT 'sent' CHECK (status IN ('sent', 'failed')),
    error_message TEXT,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alert_log_incident ON alert_log (incident_id, channel_id, event_type);
CREATE INDEX idx_alert_channels_enabled ON alert_channels (enabled) WHERE enabled = TRUE;
