
-- Static lookup tables. Seeded once, never mutated by the application.

CREATE TABLE providers (
    id            VARCHAR(50)  PRIMARY KEY,
    display_name  VARCHAR(100) NOT NULL,
    logo_url      TEXT,
    description   TEXT,
    website       TEXT,
    founded_year  INT,
    hq_location   VARCHAR(100),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE regions (
    id            VARCHAR(50)  PRIMARY KEY,
    display_name  VARCHAR(100) NOT NULL,
    flag_emoji    VARCHAR(10),
    probe_host    VARCHAR(255)
);

CREATE TABLE fee_tiers (
    id            VARCHAR(50)  PRIMARY KEY,
    lamports      BIGINT       NOT NULL,
    display_name  VARCHAR(50)  NOT NULL,
    sort_order    INT          NOT NULL
);

-- ── Seed data ────────────────────────────────────────────────────────────────

INSERT INTO providers (id, display_name, logo_url, description, website, founded_year, hq_location) VALUES
    ('helius',    'Helius',    NULL, 'The complete Solana developer platform',              'https://helius.dev',      2022, 'San Francisco, CA'),
    -- ('quicknode', 'QuickNode', NULL, 'Web3 infrastructure for everyone',                   'https://quicknode.com',   2017, 'Miami, FL'),
    ('triton',    'Triton',    NULL, 'Enterprise-grade Solana infrastructure with Yellowstone gRPC', 'https://triton.one', 2021, 'Remote'),
    ('alchemy',   'Alchemy',   NULL, 'The web3 development platform powering millions of users', 'https://alchemy.com', 2017, 'San Francisco, CA');

INSERT INTO regions (id, display_name, flag_emoji, probe_host) VALUES
    ('us-east',    'US East (Virginia)',     '🇺🇸', NULL),
    ('us-west',    'US West (Oregon)',       '🇺🇸', NULL),
    ('eu-west',    'EU West (Frankfurt)',    '🇩🇪', NULL),
    ('eu-north',   'EU North (London)',      '🇬🇧', NULL),
    ('ap-sg',      'Asia (Singapore)',       '🇸🇬', NULL),
    ('ap-tokyo',   'Asia (Tokyo)',           '🇯🇵', NULL),
    ('sa-saopaulo','South America (São Paulo)', '🇧🇷', NULL);

INSERT INTO fee_tiers (id, lamports, display_name, sort_order) VALUES
    ('none',   0,       'No Fee',  1),
    ('medium', 5000,    'Medium',  2),
    ('high',   50000,   'High',    3),
    ('turbo',  500000,  'Turbo',   4);