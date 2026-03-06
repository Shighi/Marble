-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Upload Sessions ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS upload_sessions (
    id          SERIAL PRIMARY KEY,
    session_id  UUID DEFAULT gen_random_uuid() UNIQUE,
    uploaded_at TIMESTAMP DEFAULT NOW(),
    card_file   VARCHAR(255),
    apm_file    VARCHAR(255),
    card_rows   INTEGER DEFAULT 0,
    apm_rows    INTEGER DEFAULT 0,
    status      VARCHAR(50) DEFAULT 'processing',
    summary     JSONB DEFAULT '{}'
);

-- ── Transactions (deduplicated by Txid) ──────────────────────────
CREATE TABLE IF NOT EXISTS transactions (
    id           SERIAL PRIMARY KEY,
    session_id   UUID REFERENCES upload_sessions(session_id) ON DELETE CASCADE,
    txid         VARCHAR(255),
    data_source  VARCHAR(10),   -- 'CARD' | 'APM'
    email        VARCHAR(500),
    card_no      VARCHAR(255),  -- masked: first6 + XXXXXX + last4
    phone        VARCHAR(100),
    amount       NUMERIC(15,2),
    amount_usd   NUMERIC(15,2),
    currency     VARCHAR(10),
    tx_status    VARCHAR(50),
    tx_type      VARCHAR(100),
    country      VARCHAR(100),
    bin_country  VARCHAR(100),
    is_3d        VARCHAR(10),
    merchant     VARCHAR(255),
    processor    VARCHAR(255),
    bank_name    VARCHAR(255),
    error_desc   TEXT,
    tx_date      TIMESTAMP,
    created_at   TIMESTAMP DEFAULT NOW(),
    UNIQUE(txid, data_source)
);

-- ── Fraud Alerts / Cases ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fraud_cases (
    id           SERIAL PRIMARY KEY,
    case_ref     VARCHAR(20) UNIQUE,          -- e.g. CASE-00042
    session_id   UUID REFERENCES upload_sessions(session_id) ON DELETE SET NULL,
    alert_type   VARCHAR(100),                -- payout_only | velocity | sanctions | 3ds_anomaly | recurring | timing
    severity     VARCHAR(20) DEFAULT 'high',  -- critical | high | medium | low
    entity_type  VARCHAR(20),                 -- email | phone | card
    entity_value VARCHAR(500),
    amount_usd   NUMERIC(15,2) DEFAULT 0,
    tx_count     INTEGER DEFAULT 0,
    details      JSONB DEFAULT '{}',
    status       VARCHAR(50) DEFAULT 'open',  -- open | confirmed_fraud | false_positive | under_investigation
    notes        TEXT,
    reviewed_by  VARCHAR(255),
    reviewed_at  TIMESTAMP,
    created_at   TIMESTAMP DEFAULT NOW()
);

-- ── Case counter sequence for nice IDs ───────────────────────────
CREATE SEQUENCE IF NOT EXISTS case_seq START 1;

-- ── Analysis Results (full JSON blob per session) ────────────────
CREATE TABLE IF NOT EXISTS analysis_results (
    id          SERIAL PRIMARY KEY,
    session_id  UUID REFERENCES upload_sessions(session_id) ON DELETE CASCADE UNIQUE,
    results     JSONB DEFAULT '{}',
    created_at  TIMESTAMP DEFAULT NOW()
);
-- ── Whitelisted Entities (Trusted Partners/Entities) ──────────
CREATE TABLE IF NOT EXISTS trusted_entities (
    id           SERIAL PRIMARY KEY,
    entity_type  VARCHAR(20),              -- 'email' | 'phone' | 'card' | 'merchant'
    entity_value VARCHAR(500) UNIQUE,
    reason       TEXT,                      -- Why this entity is trusted
    whitelisted_by VARCHAR(255),           -- Username/ID of reviewer
    whitelisted_at TIMESTAMP DEFAULT NOW(),
    notes        TEXT
);

-- ── Index for fast lookups ──────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_trusted_type_value ON trusted_entities(entity_type, entity_value);
-- ── Indexes ───────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_transactions_email    ON transactions(email);
CREATE INDEX IF NOT EXISTS idx_transactions_phone    ON transactions(phone);
CREATE INDEX IF NOT EXISTS idx_transactions_card     ON transactions(card_no);
CREATE INDEX IF NOT EXISTS idx_transactions_txid     ON transactions(txid);
CREATE INDEX IF NOT EXISTS idx_transactions_session  ON transactions(session_id);
CREATE INDEX IF NOT EXISTS idx_fraud_entity          ON fraud_cases(entity_value);
CREATE INDEX IF NOT EXISTS idx_fraud_status          ON fraud_cases(status);
CREATE INDEX IF NOT EXISTS idx_fraud_session         ON fraud_cases(session_id);
