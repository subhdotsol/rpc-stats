-- Add migration script here

CREATE TYPE rpc_type AS ENUM ('http', 'websocket');
 
CREATE TABLE rpcs (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    chain_id      UUID        NOT NULL REFERENCES chains (id) ON DELETE CASCADE,
    provider      TEXT        NOT NULL,
    url           TEXT        NOT NULL UNIQUE,
    rpc_type      rpc_type    NOT NULL DEFAULT 'http',
    is_active     BOOLEAN     NOT NULL DEFAULT TRUE,
    tags          JSONB       NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at    TIMESTAMPTZ
);
 
CREATE INDEX idx_rpcs_chain_id   ON rpcs (chain_id);
CREATE INDEX idx_rpcs_is_active  ON rpcs (is_active) WHERE deleted_at IS NULL;
CREATE INDEX idx_rpcs_tags       ON rpcs USING GIN (tags);
 
CREATE TRIGGER rpcs_updated_at
    BEFORE UPDATE ON rpcs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();