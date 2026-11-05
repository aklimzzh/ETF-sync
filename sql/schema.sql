BEGIN;

-- ============================================================
-- Tables
-- ============================================================

CREATE TABLE IF NOT EXISTS etf_master (
    id BIGSERIAL PRIMARY KEY,
    exchange VARCHAR(4) NOT NULL
        CHECK (exchange IN ('SSE', 'SZSE')),
    fund_code VARCHAR(10) NOT NULL,
    fund_name VARCHAR(100) NOT NULL,
    etf_type VARCHAR(30),
    first_seen_date DATE NOT NULL,
    last_seen_date DATE NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_etf_exchange_code
        UNIQUE (exchange, fund_code)
);

CREATE TABLE IF NOT EXISTS etf_share_daily (
    etf_id BIGINT NOT NULL
        REFERENCES etf_master(id),
    trade_date DATE NOT NULL,
    total_share NUMERIC(20,4) NOT NULL
        CHECK (total_share >= 0),
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source VARCHAR(20) NOT NULL,
    PRIMARY KEY (etf_id, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_etf_share_trade_date
    ON etf_share_daily(trade_date);

CREATE INDEX IF NOT EXISTS idx_etf_share_etf_date_desc
    ON etf_share_daily(etf_id, trade_date DESC);

CREATE TABLE IF NOT EXISTS import_log (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    exchange VARCHAR(4) NOT NULL
        CHECK (exchange IN ('SSE', 'SZSE')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    record_count INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL
        CHECK (status IN ('STARTED', 'SUCCESS', 'FAILED', 'SKIPPED')),
    github_run_id VARCHAR(50),
    github_commit VARCHAR(40),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_import_log_date_exchange
        UNIQUE (trade_date, exchange)
);


-- ============================================================
-- Base View
-- ============================================================

CREATE OR REPLACE VIEW v_etf_share_daily AS
SELECT
    s.trade_date,
    e.exchange,
    e.fund_code,
    e.fund_name,
    e.etf_type,
    s.total_share,
    s.source,
    s.fetched_at
FROM etf_share_daily s
JOIN etf_master e
    ON e.id = s.etf_id;


-- ============================================================
-- ETF Daily Change
-- ============================================================

CREATE OR REPLACE VIEW v_etf_share_change AS
WITH h AS (
    SELECT
        s.trade_date,
        e.id AS etf_id,
        e.exchange,
        e.fund_code,
        e.fund_name,
        e.etf_type,
        s.total_share,
        LAG(s.total_share) OVER (
            PARTITION BY e.id
            ORDER BY s.trade_date
        ) AS previous_share
    FROM etf_share_daily s
    JOIN etf_master e
        ON e.id = s.etf_id
)
SELECT
    trade_date,
    exchange,
    fund_code,
    fund_name,
    etf_type,
    total_share,
    previous_share,
    total_share - previous_share AS share_change,
    CASE WHEN previous_share > 0
        THEN (total_share / previous_share - 1) * 100
    END AS share_change_pct
FROM h;


-- ============================================================
-- ETF 1D / 5D / 20D Metrics
-- ============================================================

CREATE OR REPLACE VIEW v_etf_share_metrics AS
WITH h AS (
    SELECT
        s.trade_date,
        e.id AS etf_id,
        e.exchange,
        e.fund_code,
        e.fund_name,
        e.etf_type,
        s.total_share,
        LAG(s.total_share, 1) OVER (
            PARTITION BY e.id ORDER BY s.trade_date
        ) AS share_1d_ago,
        LAG(s.total_share, 5) OVER (
            PARTITION BY e.id ORDER BY s.trade_date
        ) AS share_5d_ago,
        LAG(s.total_share, 20) OVER (
            PARTITION BY e.id ORDER BY s.trade_date
        ) AS share_20d_ago
    FROM etf_share_daily s
    JOIN etf_master e
        ON e.id = s.etf_id
)
SELECT
    trade_date,
    exchange,
    fund_code,
    fund_name,
    etf_type,
    total_share,

    share_1d_ago,
    total_share - share_1d_ago AS change_1d,
    CASE WHEN share_1d_ago > 0
        THEN (total_share / share_1d_ago - 1) * 100
    END AS change_1d_pct,

    share_5d_ago,
    total_share - share_5d_ago AS change_5d,
    CASE WHEN share_5d_ago > 0
        THEN (total_share / share_5d_ago - 1) * 100
    END AS change_5d_pct,

    share_20d_ago,
    total_share - share_20d_ago AS change_20d,
    CASE WHEN share_20d_ago > 0
        THEN (total_share / share_20d_ago - 1) * 100
    END AS change_20d_pct
FROM h;


-- ============================================================
-- Market Summary: SSE / SZSE / ALL
-- ============================================================

CREATE OR REPLACE VIEW v_market_daily_summary AS
WITH by_exchange AS (
    SELECT
        s.trade_date,
        e.exchange,
        COUNT(*)::BIGINT AS etf_count,
        SUM(s.total_share) AS total_share
    FROM etf_share_daily s
    JOIN etf_master e
        ON e.id = s.etf_id
    GROUP BY s.trade_date, e.exchange
),
market AS (
    SELECT * FROM by_exchange

    UNION ALL

    SELECT
        trade_date,
        'ALL'::VARCHAR(4),
        SUM(etf_count)::BIGINT,
        SUM(total_share)
    FROM by_exchange
    GROUP BY trade_date
),
h AS (
    SELECT
        *,
        LAG(total_share, 1) OVER (
            PARTITION BY exchange ORDER BY trade_date
        ) AS total_share_1d_ago,
        LAG(total_share, 5) OVER (
            PARTITION BY exchange ORDER BY trade_date
        ) AS total_share_5d_ago,
        LAG(total_share, 20) OVER (
            PARTITION BY exchange ORDER BY trade_date
        ) AS total_share_20d_ago
    FROM market
)
SELECT
    trade_date,
    exchange,
    etf_count,
    total_share,

    total_share_1d_ago AS previous_total_share,
    total_share - total_share_1d_ago AS change_1d,
    CASE WHEN total_share_1d_ago > 0
        THEN (total_share / total_share_1d_ago - 1) * 100
    END AS change_1d_pct,

    total_share_5d_ago,
    total_share - total_share_5d_ago AS change_5d,
    CASE WHEN total_share_5d_ago > 0
        THEN (total_share / total_share_5d_ago - 1) * 100
    END AS change_5d_pct,

    total_share_20d_ago,
    total_share - total_share_20d_ago AS change_20d,
    CASE WHEN total_share_20d_ago > 0
        THEN (total_share / total_share_20d_ago - 1) * 100
    END AS change_20d_pct
FROM h;

COMMIT;
