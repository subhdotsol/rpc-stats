-- Add up migration script here

-- Raw transaction records. Written at Kafka tx.submitted event.
-- tx_results is updated progressively as tx.landed and tx.confirmed arrive.

CREATE TABLE transactions (
    id             BIGSERIAL    PRIMARY KEY,
    signature      VARCHAR(100) NOT NULL UNIQUE,
    provider_id    VARCHAR(50)  NOT NULL REFERENCES providers(id),
    region_id      VARCHAR(50)  NOT NULL REFERENCES regions(id),
    fee_tier_id    VARCHAR(50)  NOT NULL REFERENCES fee_tiers(id),
    submitted_at   TIMESTAMPTZ  NOT NULL,
    submitted_slot BIGINT,
    -- Snapshot of chain TPS at the exact moment of submission.
    -- Used later by the congestion analysis page.
    network_tps    INT,
    -- Groups all provider submissions that happened in the same 30-second sweep.
    -- One batch_id = one round of simultaneous testing across all providers.
    batch_id       UUID         NOT NULL
);

CREATE INDEX idx_tx_provider_time  ON transactions (provider_id, submitted_at DESC);
CREATE INDEX idx_tx_batch          ON transactions (batch_id);
CREATE INDEX idx_tx_signature      ON transactions (signature);
CREATE INDEX idx_tx_region_time    ON transactions (region_id, submitted_at DESC);
CREATE INDEX idx_tx_fee_provider   ON transactions (fee_tier_id, provider_id);
CREATE INDEX idx_tx_submitted_at   ON transactions (submitted_at DESC);

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE tx_results (
    transaction_id        BIGINT       PRIMARY KEY REFERENCES transactions(id) ON DELETE CASCADE,
    -- Denormalized for fast per-provider queries without joining transactions.
    signature             VARCHAR(100) NOT NULL,
    provider_id           VARCHAR(50)  NOT NULL,

    -- Lifecycle state of this transaction.
    -- pending  → waiting for Geyser confirmation or timeout
    -- landed   → Geyser confirmed on-chain inclusion
    -- dropped  → transaction did not land (block excluded or expired)
    -- timeout  → no response within the observation window (currently 30s)
    status                VARCHAR(20)  NOT NULL DEFAULT 'pending',

    -- ── Geyser (source of truth) ─────────────────────────────────────────────
    -- Set when Kafka tx.landed event arrives from the Geyser plugin.
    geyser_landed_at      TIMESTAMPTZ,
    landed_slot           BIGINT,
    -- Calculated by the stat engine worker:
    --   landing_time_ms = EXTRACT(EPOCH FROM geyser_landed_at - submitted_at) * 1000
    landing_time_ms       INT,

    -- ── RPC reported confirmation ─────────────────────────────────────────────
    -- Set when Kafka tx.confirmed event arrives (RPC poll / subscription).
    rpc_confirmed_at      TIMESTAMPTZ,
    -- Calculated by the stat engine worker:
    --   rpc_confirm_time_ms = EXTRACT(EPOCH FROM rpc_confirmed_at - submitted_at) * 1000
    rpc_confirm_time_ms   INT,

    -- ── The lie metric ────────────────────────────────────────────────────────
    -- Computed automatically by trigger when both values are present.
    -- Positive → RPC said "confirmed" before Geyser saw the tx on-chain (lying fast).
    -- Negative → Geyser saw it before RPC reported (RPC was slow to notify).
    claim_vs_reality_ms   INT,

    -- How many slots behind the chain tip the provider's node was at submit time.
    -- Captured by the worker at tx.submitted:
    --   slot_lag = chain_tip_slot - provider_current_slot
    slot_lag_at_submit    DECIMAL(10, 2),

    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_txr_provider_time   ON tx_results (provider_id, updated_at DESC);
CREATE INDEX idx_txr_pending         ON tx_results (status)              WHERE status = 'pending';
CREATE INDEX idx_txr_provider_status ON tx_results (provider_id, status);
CREATE INDEX idx_txr_landed_at       ON tx_results (geyser_landed_at DESC) WHERE geyser_landed_at IS NOT NULL;