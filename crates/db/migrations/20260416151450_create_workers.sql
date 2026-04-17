-- Add migration script here
CREATE TYPE worker_status AS ENUM ('active', 'idle', 'dead');
 
CREATE TABLE workers (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    region              TEXT          NOT NULL,
    hostname            TEXT          NOT NULL,
    status              worker_status NOT NULL DEFAULT 'idle',
    last_heartbeat_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
 
CREATE INDEX idx_workers_region ON workers (region);
CREATE INDEX idx_workers_status ON workers (status);
 
-- unique active worker per region+hostname
CREATE UNIQUE INDEX idx_workers_region_hostname ON workers (region, hostname);
 