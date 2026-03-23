"""RAG context retriever utilities backed by Postgres via SQLAlchemy ORM."""

from __future__ import annotations

import logging
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Generator

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

# Ensure project root is importable when this file is run directly.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from shared.db_models import Anomaly, Forecast, StockPrice, get_db  # noqa: E402


logger = logging.getLogger(__name__)


@contextmanager
def db_session() -> Generator[Session, None, None]:
    """Provide a DB session from the shared get_db generator."""
    generator = get_db()
    db = next(generator)
    try:
        yield db
    finally:
        generator.close()


def _as_float(value: Decimal | float | int | None) -> float:
    if value is None:
        return 0.0
    return float(value)


def fetch_and_store_if_missing(ticker: str, days: int = 60) -> None:
    import httpx
    import pandas as pd
    from datetime import datetime, timedelta
    import time
    from sqlalchemy import text

    db = next(get_db())
    try:
        # Check if data exists
        count = db.execute(
            text("SELECT COUNT(*) FROM stock_prices WHERE ticker = :ticker"),
            {"ticker": ticker},
        ).scalar()

        if count > 0:
            return

        logging.info(f"Fetching data for {ticker} via direct Yahoo Finance API...")

        # Calculate timestamps
        end = int(time.time())
        start = int((datetime.now() - timedelta(days=days)).timestamp())

        # Direct Yahoo Finance API call with browser headers
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://finance.yahoo.com",
            "Origin": "https://finance.yahoo.com",
        }

        params = {
            "period1": start,
            "period2": end,
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "true",
        }

        # Try query1 first, fallback to query2
        for base_url in [
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
            f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}",
        ]:
            try:
                response = httpx.get(
                    base_url,
                    headers=headers,
                    params=params,
                    timeout=30.0,
                    follow_redirects=True,
                )

                if response.status_code == 200:
                    data = response.json()
                    break
                if response.status_code == 429:
                    logging.warning(f"Rate limited on {base_url}, trying fallback...")
                    time.sleep(2)
                    continue
            except Exception as e:
                logging.error(f"Request failed: {e}")
                continue
        else:
            logging.warning(f"All Yahoo Finance endpoints failed for {ticker}")
            return

        # Parse the response
        result = data.get("chart", {}).get("result", [])
        if not result:
            logging.warning(f"No data in response for {ticker}")
            return

        chart = result[0]
        timestamps = chart.get("timestamp", [])
        indicators = chart.get("indicators", {}).get("quote", [{}])[0]

        opens = indicators.get("open", [])
        highs = indicators.get("high", [])
        lows = indicators.get("low", [])
        closes = indicators.get("close", [])
        volumes = indicators.get("volume", [])

        if not timestamps or not closes:
            logging.warning(f"Empty data for {ticker}")
            return

        # Store in Postgres
        stored = 0
        for i, ts in enumerate(timestamps):
            try:
                if closes[i] is None:
                    continue
                dt = datetime.fromtimestamp(ts)
                db.execute(
                    text(
                        """
                    INSERT INTO stock_prices
                    (ticker, timestamp, open, high, low, close, volume, source)
                    VALUES (:ticker, :timestamp, :open, :high, :low, :close, :volume, :source)
                    ON CONFLICT (ticker, timestamp) DO NOTHING
                    """
                    ),
                    {
                        "ticker": ticker,
                        "timestamp": dt,
                        "open": float(opens[i]) if opens[i] else None,
                        "high": float(highs[i]) if highs[i] else None,
                        "low": float(lows[i]) if lows[i] else None,
                        "close": float(closes[i]),
                        "volume": int(volumes[i]) if volumes[i] else 0,
                        "source": "yahoo_direct",
                    },
                )
                stored += 1
            except Exception:
                continue

        db.commit()
        logging.info(f"Stored {stored} days of data for {ticker}")

    except Exception as e:
        logging.error(f"Error in fetch_and_store_if_missing for {ticker}: {e}")
        if db:
            db.rollback()
    finally:
        db.close()


def get_price_history(ticker: str, days: int = 30) -> str:
    """Return last N days of daily average close as a formatted table string."""
    fetch_and_store_if_missing(ticker, days)
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(days, 1))

    try:
        with db_session() as db:
            stmt = (
                select(
                    func.date(StockPrice.timestamp).label("trade_date"),
                    func.avg(StockPrice.close).label("avg_close"),
                )
                .where(StockPrice.ticker == ticker, StockPrice.timestamp >= cutoff)
                .group_by(func.date(StockPrice.timestamp))
                .order_by(func.date(StockPrice.timestamp))
            )
            rows = db.execute(stmt).all()

        if not rows:
            return f"No price history available for {ticker}"

        lines = ["Date | Close"]
        for trade_date, avg_close in rows:
            lines.append(f"{trade_date} | ${_as_float(avg_close):.2f}")
        return "\n".join(lines)
    except Exception:
        logger.exception("Failed to fetch price history for %s", ticker)
        return f"No price history available for {ticker}"


def get_latest_forecast(ticker: str) -> str:
    """Return next 7 forecast rows for ticker in prompt-ready format."""
    fetch_and_store_if_missing(ticker)
    try:
        with db_session() as db:
            stmt = (
                select(
                    Forecast.forecast_date,
                    Forecast.predicted_price,
                    Forecast.lower_bound,
                    Forecast.upper_bound,
                )
                .where(Forecast.ticker == ticker, Forecast.forecast_date >= func.current_date())
                .order_by(Forecast.forecast_date.asc())
                .limit(7)
            )
            rows = db.execute(stmt).all()

        if not rows:
            return "No forecast available"

        lines = [f"Forecast for {ticker}:"]
        for forecast_date, predicted, lower, upper in rows:
            lines.append(
                f"{forecast_date}: ${_as_float(predicted):.2f} "
                f"(range ${_as_float(lower):.2f}-${_as_float(upper):.2f})"
            )
        return "\n".join(lines)
    except Exception:
        logger.exception("Failed to fetch forecast for %s", ticker)
        return "No forecast available"


def get_recent_anomalies(ticker: str, days: int = 14) -> str:
    """Return recent anomaly list for a ticker in prompt-ready format."""
    fetch_and_store_if_missing(ticker)
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(days, 1))

    try:
        with db_session() as db:
            stmt = (
                select(Anomaly.timestamp, Anomaly.anomaly_type, Anomaly.close, Anomaly.zscore)
                .where(Anomaly.ticker == ticker, Anomaly.timestamp >= cutoff)
                .order_by(Anomaly.timestamp.desc())
            )
            rows = db.execute(stmt).all()

        if not rows:
            return "No anomalies detected recently"

        lines = ["Recent anomalies:"]
        for ts, anomaly_type, close, zscore in rows:
            ts_text = ts.date().isoformat() if isinstance(ts, datetime) else str(ts)
            lines.append(
                f"{ts_text}: {anomaly_type or 'unknown'} | "
                f"price=${_as_float(close):.2f} | zscore={_as_float(zscore):.2f}"
            )
        return "\n".join(lines)
    except Exception:
        logger.exception("Failed to fetch recent anomalies for %s", ticker)
        return "No anomalies detected recently"


def get_comparison_context(ticker_a: str, ticker_b: str) -> tuple[str, str]:
    """Return price history context strings for two tickers."""
    return get_price_history(ticker_a), get_price_history(ticker_b)
