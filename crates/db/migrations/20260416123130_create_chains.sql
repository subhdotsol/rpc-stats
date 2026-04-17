CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
 
CREATE TABLE chains (
    id            UUID        PRIMARY KEY     DEFAULT uuid_generate_v4(),
    name          TEXT        NOT NULL,
    chain_id      BIGINT      NOT NULL UNIQUE,
    block_time_ms INT         NOT NULL DEFAULT 12000,
    is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
 
CREATE INDEX idx_chains_chain_id ON chains (chain_id);
 
-- auto-update updated_at
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
 
CREATE TRIGGER chains_updated_at
    BEFORE UPDATE ON chains
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();