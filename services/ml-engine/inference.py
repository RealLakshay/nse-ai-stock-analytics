"""Daily forecasting for Indian NSE stocks using Prophet + Ollama Mistral."""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

import joblib
import pandas as pd
import requests
from sqlalchemy import create_engine, text

# Ensure project root is importable when this file is run directly.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import shared.constants as constants_module  # noqa: E402
from shared.constants import (  # noqa: E402
    DATABASE_URL,
    FORECAST_DAYS,
    MARKET_TZ,
    MODEL_DIR,
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
)
from shared.ticker_registry import TickerRegistry  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

LAST_CLOSE_SQL = text(
    """
    SELECT close
    FROM stock_prices
    WHERE ticker = :ticker
    ORDER BY timestamp DESC
    LIMIT 1
    """
)

UPSERT_FORECAST_SQL = text(
    """
    INSERT INTO forecasts (
        ticker,
        forecast_date,
        predicted_price,
        lower_bound,
        upper_bound,
        model_version
    )
    VALUES (
        :ticker,
        :forecast_date,
        :predicted_price,
        :lower_bound,
        :upper_bound,
        :model_version
    )
    ON CONFLICT (ticker, forecast_date)
    DO UPDATE SET
        predicted_price = EXCLUDED.predicted_price
    """
)


def _holiday_map_from_constants() -> Dict[int, Set[str]]:
    holidays: Dict[int, Set[str]] = {}
    for name in dir(constants_module):
        if not name.startswith("NSE_HOLIDAYS_"):
            continue
        suffix = name.split("NSE_HOLIDAYS_")[-1]
        if not suffix.isdigit():
            continue
        year = int(suffix)
        value = getattr(constants_module, name, set())
        if isinstance(value, set):
            holidays[year] = {str(item) for item in value}
    return holidays


NSE_HOLIDAYS_BY_YEAR = _holiday_map_from_constants()


def get_trading_days(n: int) -> List[date]:
    """Return next n NSE trading dates from today, skipping weekends/holidays."""
    if n <= 0:
        return []

    trading_days: List[date] = []
    current = datetime.now(MARKET_TZ).date()

    # Forecast future days from tomorrow onward.
    current = current + timedelta(days=1)
    while len(trading_days) < n:
        if current.weekday() < 5:
            year_holidays = NSE_HOLIDAYS_BY_YEAR.get(current.year, set())
            if current.isoformat() not in year_holidays:
                trading_days.append(current)
        current += timedelta(days=1)

    return trading_days


def _load_model(ticker: str):
    model_path = MODEL_DIR / f"{ticker}_prophet.pkl"
    if not model_path.exists():
        logger.info("Model not found for %s, skipping.", ticker)
        return None
    return joblib.load(model_path)


def _get_last_close(engine, ticker: str) -> Optional[float]:
    with engine.connect() as conn:
        row = conn.execute(LAST_CLOSE_SQL, {"ticker": ticker}).fetchone()
    if not row:
        return None
    try:
        return float(row[0])
    except (TypeError, ValueError):
        return None


def _upsert_forecasts(engine, ticker: str, forecast_df: pd.DataFrame) -> None:
    records = []
    for _, row in forecast_df.iterrows():
        records.append(
            {
                "ticker": ticker,
                "forecast_date": row["ds"].date(),
                "predicted_price": float(row["yhat"]),
                "lower_bound": float(row["yhat_lower"]),
                "upper_bound": float(row["yhat_upper"]),
                "model_version": "prophet_v1",
            }
        )

    if not records:
        return

    with engine.begin() as conn:
        conn.execute(UPSERT_FORECAST_SQL, records)


def _build_forecast_summary(ticker: str, last_close: Optional[float], forecast_df: pd.DataFrame) -> str:
    last_close_text = (
        f"\N{INDIAN RUPEE SIGN}{last_close:.2f}" if last_close is not None else "N/A"
    )
    lines = [
        f"Stock: {ticker} (NSE, India). Currency: Indian Rupees (\N{INDIAN RUPEE SIGN}).",
        f"Last closing price: {last_close_text}",
        f"{FORECAST_DAYS}-day forecast:",
    ]

    for _, row in forecast_df.iterrows():
        ds = row["ds"].date().isoformat()
        yhat = float(row["yhat"])
        lower = float(row["yhat_lower"])
        upper = float(row["yhat_upper"])
        lines.append(
            f"{ds}: \N{INDIAN RUPEE SIGN}{yhat:.2f} "
            f"(range \N{INDIAN RUPEE SIGN}{lower:.2f}\u2013\N{INDIAN RUPEE SIGN}{upper:.2f})"
        )

    return "\n".join(lines)


def _generate_analyst_note(forecast_summary: str) -> str:
    prompt = (
        f"{forecast_summary}\n\n"
        "You are an Indian equity market analyst. "
        "Write a concise 3-sentence note on this NSE stock's predicted trend, "
        "mention the confidence range in rupees, and state 2 key risk factors "
        "relevant to Indian markets (RBI policy, FII flows, rupee depreciation etc). "
        "Be specific with \N{INDIAN RUPEE SIGN} numbers."
    )

    endpoint = f"{OLLAMA_BASE_URL.rstrip('/')}/api/generate"
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
    }

    response = requests.post(endpoint, json=payload, timeout=120)
    response.raise_for_status()
    body = response.json()
    return str(body.get("response", "")).strip()


def _run_inference_for_ticker(engine, ticker: str, trading_days: List[date]) -> None:
    model = _load_model(ticker)
    if model is None:
        return

    future_df = pd.DataFrame({"ds": pd.to_datetime(trading_days)})
    forecast = model.predict(future_df)

    # Keep only required output columns and only future dates.
    allowed = set(trading_days)
    forecast = forecast[forecast["ds"].dt.date.isin(allowed)][["ds", "yhat", "yhat_lower", "yhat_upper"]]
    if forecast.empty:
        logger.warning("No forecast rows produced for %s.", ticker)
        return

    _upsert_forecasts(engine, ticker, forecast)

    last_close = _get_last_close(engine, ticker)
    summary = _build_forecast_summary(ticker, last_close, forecast)

    try:
        note = _generate_analyst_note(summary)
        if note:
            logger.info("Analyst note | %s | %s", ticker, note)
        else:
            logger.warning("Empty analyst note received for %s", ticker)
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Failed to generate analyst note for %s: %s", ticker, exc)


def run_inference_for_all() -> None:
    """Run daily inference for all ML universe tickers."""
    registry = TickerRegistry()
    tickers = registry.get_tickers_for_ml()
    if not tickers:
        logger.warning("No tickers available for inference.")
        return

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    trading_days = get_trading_days(FORECAST_DAYS)
    logger.info("Running inference for %s tickers | horizon=%s", len(tickers), len(trading_days))

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    try:
        for ticker in tickers:
            try:
                _run_inference_for_ticker(engine, ticker, trading_days)
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Inference failed for %s: %s", ticker, exc)
    finally:
        engine.dispose()


if __name__ == "__main__":
    run_inference_for_all()
