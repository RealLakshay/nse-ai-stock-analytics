-- 004_forecasts.sql

CREATE TABLE IF NOT EXISTS forecasts (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    forecast_date DATE NOT NULL,
    predicted_price NUMERIC(12,4),
    lower_bound NUMERIC(12,4),
    upper_bound NUMERIC(12,4),
    model_version VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_forecasts_ticker_forecast_date'
          AND conrelid = 'forecasts'::regclass
    ) THEN
        ALTER TABLE forecasts
            ADD CONSTRAINT uq_forecasts_ticker_forecast_date
            UNIQUE (ticker, forecast_date);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_forecasts_ticker_forecast_date
    ON forecasts (ticker, forecast_date);
