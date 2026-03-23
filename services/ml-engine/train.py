"""Weekly Prophet model training for Indian NSE stocks."""

from __future__ import annotations

import logging
import sys
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Dict, List

import joblib
import pandas as pd
from prophet import Prophet
from sqlalchemy import create_engine, text

# Ensure project root is importable when this file is run directly.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from shared.constants import DATABASE_URL, FORECAST_DAYS, MARKET_TZ, ML_BATCH_SIZE, MODEL_DIR  # noqa: E402
from shared.ticker_registry import TickerRegistry  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

MARKET_TZ_NAME = getattr(MARKET_TZ, "zone", "Asia/Kolkata")

PRICE_QUERY = text(
    """
    SELECT DATE(timestamp AT TIME ZONE :market_tz) AS ds,
           AVG(close) AS y
    FROM stock_prices
    WHERE ticker = :ticker
      AND timestamp > NOW() - INTERVAL '365 days'
    GROUP BY ds
    ORDER BY ds
    """
)


def _chunked(items: List[str], chunk_size: int) -> List[List[str]]:
    size = max(1, int(chunk_size))
    return [items[i : i + size] for i in range(0, len(items), size)]


def _load_training_frame(ticker: str) -> pd.DataFrame:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        df = pd.read_sql_query(
            PRICE_QUERY,
            con=engine,
            params={"ticker": ticker, "market_tz": MARKET_TZ_NAME},
        )
    finally:
        engine.dispose()

    if df.empty:
        return df

    df["ds"] = pd.to_datetime(df["ds"], errors="coerce")
    df["y"] = pd.to_numeric(df["y"], errors="coerce")
    df = df.dropna(subset=["ds", "y"]).sort_values("ds")
    return df


def _train_single_ticker(ticker: str) -> Dict:
    try:
        df = _load_training_frame(ticker)
        row_count = len(df)
        if row_count < 60:
            return {"status": "skipped", "ticker": ticker, "rows": row_count}

        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
            changepoint_prior_scale=0.05,
        )
        model.add_seasonality(name="diwali_season", period=354.37, fourier_order=5)
        model.fit(df[["ds", "y"]])

        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        model_path = MODEL_DIR / f"{ticker}_prophet.pkl"
        joblib.dump(model, model_path)

        last_price = float(df["y"].iloc[-1])
        return {
            "status": "trained",
            "ticker": ticker,
            "rows": row_count,
            "last_price": last_price,
            "model_path": str(model_path),
        }
    except Exception as exc:  # pylint: disable=broad-except
        return {"status": "error", "ticker": ticker, "error": str(exc)}


def train_all_tickers() -> None:
    """Train Prophet models for ML universe tickers in parallel batches."""
    registry = TickerRegistry()
    tickers = registry.get_tickers_for_ml()

    if not tickers:
        logger.warning("No tickers found for ML training.")
        return

    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    batches = _chunked(tickers, ML_BATCH_SIZE)
    trained = skipped = errors = 0

    logger.info(
        "Starting weekly training for %s tickers in %s batch(es) | horizon=%s days",
        len(tickers),
        len(batches),
        FORECAST_DAYS,
    )

    for idx, batch in enumerate(batches, start=1):
        process_count = max(1, min(len(batch), cpu_count()))
        logger.info("Batch %s/%s | tickers=%s | workers=%s", idx, len(batches), len(batch), process_count)

        with Pool(processes=process_count) as pool:
            results = pool.map(_train_single_ticker, batch)

        for result in results:
            status = result.get("status")
            if status == "trained":
                trained += 1
                logger.info(
                    "Trained %s on %s rows \u2192 \N{INDIAN RUPEE SIGN}%.2f last close",
                    result["ticker"],
                    result["rows"],
                    result["last_price"],
                )
            elif status == "skipped":
                skipped += 1
                logger.info("Skipped %s (rows=%s < 60)", result["ticker"], result["rows"])
            else:
                errors += 1
                logger.error("Failed %s | error=%s", result.get("ticker"), result.get("error"))

    logger.info(
        "Training complete | trained=%s skipped=%s errors=%s total=%s",
        trained,
        skipped,
        errors,
        len(tickers),
    )


if __name__ == "__main__":
    train_all_tickers()
