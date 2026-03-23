-- 002_stock_prices.sql

CREATE TABLE IF NOT EXISTS stock_prices (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC(12,4),
    high NUMERIC(12,4),
    low NUMERIC(12,4),
    close NUMERIC(12,4) NOT NULL,
    volume BIGINT,
    source VARCHAR(20),
    exchange VARCHAR(5) DEFAULT 'NSE'
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_stock_prices_ticker_timestamp'
          AND conrelid = 'stock_prices'::regclass
    ) THEN
        ALTER TABLE stock_prices
            ADD CONSTRAINT uq_stock_prices_ticker_timestamp
            UNIQUE (ticker, timestamp);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_timestamp
    ON stock_prices (ticker, timestamp);
