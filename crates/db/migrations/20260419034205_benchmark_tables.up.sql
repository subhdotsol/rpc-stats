-- Add up migration script here

-- Tables for the Benchmark Suite pages and Provider Directory.

-- ── RPC Method benchmarks ─────────────────────────────────────────────────────
-- Written by a dedicated method-probe service (separate from the tx pipeline).
-- Calls each JSON-RPC method directly and records latency.
-- Powers the "RPC Methods" tab in the Benchmark Suite.


CREATE TABLE rpc_method_metrics (
    time          TIMESTAMPTZ   NOT NULL,
    provider_id   VARCHAR(50)   NOT NULL REFERENCES providers(id),
    region_id     VARCHAR(50)   NOT NULL REFERENCES regions(id),

    -- e.g. 'sendTransaction', 'getLatestBlockhash', 'getBalance', 'getSlot'
    method_name   VARCHAR(100)  NOT NULL,

    -- 'READ' | 'WRITE' | 'SIMULATE'
    method_type   VARCHAR(20)   NOT NULL CHECK (method_type IN ('READ', 'WRITE', 'SIMULATE')),

    p50_ms        INT,
    p95_ms        INT,
    p99_ms        INT,

    -- Fraction of calls that returned an error (not a timeout): 0.01300 = 1.3%
    error_rate    DECIMAL(6, 5),

    sample_count  INT           NOT NULL DEFAULT 0
);

SELECT create_hypertable(
    'rpc_method_metrics',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX ON rpc_method_metrics (provider_id, method_name, time DESC);
CREATE INDEX ON rpc_method_metrics (method_name, time DESC);

-- ── WebSocket / gRPC stream benchmarks ───────────────────────────────────────
-- Written by a persistent connection probe measuring streaming delivery.
-- Powers the "WS / gRPC Streams" tab in the Benchmark Suite.

CREATE TABLE ws_stream_metrics (
    time             TIMESTAMPTZ   NOT NULL,
    provider_id      VARCHAR(50)   NOT NULL REFERENCES providers(id),
    region_id        VARCHAR(50)   NOT NULL REFERENCES regions(id),

    -- 'account_subscribe'      → WebSocket account change notifications
    -- 'slot_subscribe'         → new slot notifications
    -- 'transaction_subscribe'  → real-time tx notifications with full detail
    -- 'block_subscribe'        → new block with full metadata
    -- 'yellowstone_grpc'       → Triton/Helius Geyser plugin gRPC interface
    stream_type      VARCHAR(80)   NOT NULL,

    avg_latency_ms   INT,
    msg_per_second   DECIMAL(10, 2),

    -- Delivery reliability percentage (e.g. 99.9, not 0.999)
    reliability_pct  DECIMAL(5, 2),

    -- FALSE when the provider does not expose this stream type at all
    is_available     BOOLEAN       NOT NULL DEFAULT TRUE
);

SELECT create_hypertable(
    'ws_stream_metrics',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX ON ws_stream_metrics (provider_id, stream_type, time DESC);

-- ── Network conditions ────────────────────────────────────────────────────────
-- Written every ~2 seconds by a dedicated chain-tip monitor.
-- Used to:
--   (a) tag transactions with network_tps at submission time
--   (b) power the "TPS Correlation" and "Congestion" tabs

CREATE TABLE network_conditions (
    time              TIMESTAMPTZ   NOT NULL,
    current_tps       INT           NOT NULL,
    current_slot      BIGINT        NOT NULL,

    -- 'low' | 'normal' | 'elevated' | 'high' | 'extreme'
    -- Derived from current_tps thresholds:
    --   low      < 1000
    --   normal   1000–3000
    --   elevated 3000–4500
    --   high     4500–5500
    --   extreme  > 5500
    congestion_level  VARCHAR(20)   NOT NULL,

    PRIMARY KEY (time)
);

SELECT create_hypertable(
    'network_conditions',
    'time',
    chunk_time_interval => INTERVAL '6 hours',
    if_not_exists       => TRUE
);

-- ── Provider features matrix ──────────────────────────────────────────────────
-- Powers the "Feature Matrix" tab in the Provider Directory.
-- Manually maintained (or updated by a feature-probe script).

CREATE TABLE provider_features (
    provider_id   VARCHAR(50)   NOT NULL REFERENCES providers(id),
    feature_name  VARCHAR(80)   NOT NULL,
    description   TEXT,
    is_supported  BOOLEAN       NOT NULL,
    -- Free-text for partial support, e.g. 'Partial DAS support'
    notes         TEXT,
    PRIMARY KEY (provider_id, feature_name)
);

-- Seed feature matrix
INSERT INTO provider_features (provider_id, feature_name, description, is_supported, notes) VALUES
    -- Helius (12/12)
    ('helius', 'JSON-RPC',           'Standard Solana JSON-RPC methods',               TRUE,  NULL),
    ('helius', 'WebSocket',          'Real-time subscription via WebSocket',            TRUE,  NULL),
    ('helius', 'Yellowstone gRPC',   'Geyser plugin gRPC streaming',                   TRUE,  NULL),
    ('helius', 'DAS API',            'Digital Asset Standard for NFT/token metadata',   TRUE,  'Full DAS + Enhanced API'),
    ('helius', 'Enhanced API',       'Enriched transaction and account data',           TRUE,  NULL),
    ('helius', 'Webhooks',           'Push notifications for on-chain events',          TRUE,  NULL),
    ('helius', 'Priority Fee API',   'Real-time priority fee estimation',               TRUE,  NULL),
    ('helius', 'Jito Bundles',       'Support for Jito MEV bundle submission',          TRUE,  NULL),
    ('helius', 'Compression API',    'Compressed NFT minting and transfer',             TRUE,  NULL),
    ('helius', 'Token Metadata',     'On-chain token metadata lookup',                  TRUE,  NULL),
    ('helius', 'Stake-Weighted QoS', 'Prioritized block inclusion via validator stake', TRUE,  NULL),
    ('helius', 'Dedicated Nodes',    'Private bare-metal RPC infrastructure',           TRUE,  NULL),

    -- QuickNode (7/12)
    ('quicknode', 'JSON-RPC',           'Standard Solana JSON-RPC methods',               TRUE,  NULL),
    ('quicknode', 'WebSocket',          'Real-time subscription via WebSocket',            TRUE,  NULL),
    ('quicknode', 'Yellowstone gRPC',   'Geyser plugin gRPC streaming',                   FALSE, NULL),
    ('quicknode', 'DAS API',            'Digital Asset Standard for NFT/token metadata',   FALSE, NULL),
    ('quicknode', 'Enhanced API',       'Enriched transaction and account data',           FALSE, NULL),
    ('quicknode', 'Webhooks',           'Push notifications for on-chain events',          TRUE,  NULL),
    ('quicknode', 'Priority Fee API',   'Real-time priority fee estimation',               TRUE,  NULL),
    ('quicknode', 'Jito Bundles',       'Support for Jito MEV bundle submission',          TRUE,  NULL),
    ('quicknode', 'Compression API',    'Compressed NFT minting and transfer',             FALSE, NULL),
    ('quicknode', 'Token Metadata',     'On-chain token metadata lookup',                  FALSE, NULL),
    ('quicknode', 'Stake-Weighted QoS', 'Prioritized block inclusion via validator stake', TRUE,  NULL),
    ('quicknode', 'Dedicated Nodes',    'Private bare-metal RPC infrastructure',           TRUE,  NULL),

    -- Triton (7/12)
    ('triton', 'JSON-RPC',           'Standard Solana JSON-RPC methods',               TRUE,  NULL),
    ('triton', 'WebSocket',          'Real-time subscription via WebSocket',            TRUE,  NULL),
    ('triton', 'Yellowstone gRPC',   'Geyser plugin gRPC streaming',                   TRUE,  NULL),
    ('triton', 'DAS API',            'Digital Asset Standard for NFT/token metadata',   FALSE, NULL),
    ('triton', 'Enhanced API',       'Enriched transaction and account data',           FALSE, NULL),
    ('triton', 'Webhooks',           'Push notifications for on-chain events',          FALSE, NULL),
    ('triton', 'Priority Fee API',   'Real-time priority fee estimation',               TRUE,  NULL),
    ('triton', 'Jito Bundles',       'Support for Jito MEV bundle submission',          TRUE,  NULL),
    ('triton', 'Compression API',    'Compressed NFT minting and transfer',             FALSE, NULL),
    ('triton', 'Token Metadata',     'On-chain token metadata lookup',                  FALSE, NULL),
    ('triton', 'Stake-Weighted QoS', 'Prioritized block inclusion via validator stake', TRUE,  NULL),
    ('triton', 'Dedicated Nodes',    'Private bare-metal RPC infrastructure',           TRUE,  NULL),

    -- Alchemy (8/12)
    ('alchemy', 'JSON-RPC',           'Standard Solana JSON-RPC methods',               TRUE,  NULL),
    ('alchemy', 'WebSocket',          'Real-time subscription via WebSocket',            TRUE,  NULL),
    ('alchemy', 'Yellowstone gRPC',   'Geyser plugin gRPC streaming',                   FALSE, NULL),
    ('alchemy', 'DAS API',            'Digital Asset Standard for NFT/token metadata',   TRUE,  'Partial DAS support'),
    ('alchemy', 'Enhanced API',       'Enriched transaction and account data',           FALSE, NULL),
    ('alchemy', 'Webhooks',           'Push notifications for on-chain events',          TRUE,  NULL),
    ('alchemy', 'Priority Fee API',   'Real-time priority fee estimation',               TRUE,  NULL),
    ('alchemy', 'Jito Bundles',       'Support for Jito MEV bundle submission',          FALSE, NULL),
    ('alchemy', 'Compression API',    'Compressed NFT minting and transfer',             TRUE,  NULL),
    ('alchemy', 'Token Metadata',     'On-chain token metadata lookup',                  TRUE,  NULL),
    ('alchemy', 'Stake-Weighted QoS', 'Prioritized block inclusion via validator stake', FALSE, NULL),
    ('alchemy', 'Dedicated Nodes',    'Private bare-metal RPC infrastructure',           TRUE,  NULL);

-- ── Provider pricing ──────────────────────────────────────────────────────────
-- Powers the "Pricing" section in the Provider Directory.
-- NULL price_usd_mo = free tier.

CREATE TABLE provider_pricing (
    provider_id     VARCHAR(50)   NOT NULL REFERENCES providers(id),
    tier_name       VARCHAR(50)   NOT NULL,       -- 'Free' | 'Starter' | 'Growth' | 'Business'
    price_usd_mo    INT,                          -- monthly cost in USD, NULL = free
    rps_limit       INT,                          -- requests per second
    request_limit   BIGINT,                       -- monthly cap, NULL = unlimited
    sort_order      INT           NOT NULL,
    PRIMARY KEY (provider_id, tier_name)
);

INSERT INTO provider_pricing (provider_id, tier_name, price_usd_mo, rps_limit, request_limit, sort_order) VALUES
    ('helius',    'Free',     NULL, 10,    100000,     1),
    ('helius',    'Starter',  49,   100,   2000000,    2),
    ('helius',    'Growth',   199,  500,   10000000,   3),
    ('helius',    'Business', 999,  2000,  100000000,  4),

    ('quicknode', 'Free',     NULL, 5,     50000,      1),
    ('quicknode', 'Starter',  29,   25,    500000,     2),
    ('quicknode', 'Growth',   149,  200,   5000000,    3),
    ('quicknode', 'Business', 499,  1000,  50000000,   4),

    ('triton',    'Free',     NULL, 2,     25000,      1),
    ('triton',    'Starter',  79,   50,    1000000,    2),
    ('triton',    'Growth',   299,  300,   10000000,   3),
    ('triton',    'Business', 1499, 5000,  NULL,       4),

    ('alchemy',   'Free',     NULL, 5,     100000,     1),
    ('alchemy',   'Starter',  49,   50,    1000000,    2),
    ('alchemy',   'Growth',   199,  250,   5000000,    3),
    ('alchemy',   'Business', 799,  1500,  75000000,   4);