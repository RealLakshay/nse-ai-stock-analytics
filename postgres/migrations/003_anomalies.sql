-- 003_anomalies.sql

CREATE TABLE IF NOT EXISTS anomalies (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    close NUMERIC(12,4),
    zscore NUMERIC(8,4),
    anomaly_type VARCHAR(30),
    alert_sent BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_ticker_timestamp
    ON anomalies (ticker, timestamp);
