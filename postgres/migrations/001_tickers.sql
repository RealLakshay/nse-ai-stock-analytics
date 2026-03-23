-- 001_tickers.sql

CREATE TABLE IF NOT EXISTS tickers (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    company_name VARCHAR(100),
    sector VARCHAR(50),
    index_name VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    last_synced TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tickers_symbol
    ON tickers (symbol);
