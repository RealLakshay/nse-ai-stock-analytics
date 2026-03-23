"""Centralized app constants loaded from environment variables."""

from __future__ import annotations

import os
from datetime import datetime, time
from pathlib import Path
from typing import Set

import pytz
from dotenv import load_dotenv
from pytz import UnknownTimeZoneError

# Resolve project root from: <root>/shared/constants.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def _parse_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_float(value: str, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _parse_int(value: str, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _parse_time(value: str, fallback: str) -> time:
    raw = (value or fallback).strip()
    try:
        return datetime.strptime(raw, "%H:%M").time()
    except ValueError:
        return datetime.strptime(fallback, "%H:%M").time()


def _parse_timezone(value: str, fallback: str):
    try:
        return pytz.timezone(value or fallback)
    except UnknownTimeZoneError:
        return pytz.timezone(fallback)


# Kafka / Redpanda
KAFKA_BROKER: str = os.getenv("KAFKA_BROKER", "localhost:19092")
KAFKA_TOPIC_RAW: str = os.getenv("KAFKA_TOPIC_RAW", "stock.prices.raw")
KAFKA_TOPIC_ANOMALIES: str = os.getenv("KAFKA_TOPIC_ANOMALIES", "stock.anomalies")

# Database
DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    "postgresql://stockuser:stockpass@localhost:5432/stockdb",
)

# Ollama / Mistral
OLLAMA_BASE_URL: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "mistral")

# Market / exchange config
EXCHANGE: str = os.getenv("EXCHANGE", "NSE")
TICKER_SUFFIX: str = os.getenv("TICKER_SUFFIX", ".NS")
MARKET_OPEN: time = _parse_time(os.getenv("MARKET_OPEN_TIME", "09:15"), "09:15")
MARKET_CLOSE: time = _parse_time(os.getenv("MARKET_CLOSE_TIME", "15:30"), "15:30")
MARKET_TZ = _parse_timezone(os.getenv("MARKET_TIMEZONE", "Asia/Kolkata"), "Asia/Kolkata")

# ML + anomaly config
ANOMALY_ZSCORE_THRESHOLD: float = _parse_float(os.getenv("ANOMALY_ZSCORE_THRESHOLD", "3.0"), 3.0)
FORECAST_DAYS: int = _parse_int(os.getenv("FORECAST_DAYS", "7"), 7)
ML_BATCH_SIZE: int = _parse_int(os.getenv("ML_BATCH_SIZE", "50"), 50)

# Broker credentials
KITE_API_KEY: str = os.getenv("KITE_API_KEY", "")
KITE_API_SECRET: str = os.getenv("KITE_API_SECRET", "")
KITE_ACCESS_TOKEN: str = os.getenv("KITE_ACCESS_TOKEN", "")

UPSTOX_API_KEY: str = os.getenv("UPSTOX_API_KEY", "")
UPSTOX_API_SECRET: str = os.getenv("UPSTOX_API_SECRET", "")
UPSTOX_ACCESS_TOKEN: str = os.getenv("UPSTOX_ACCESS_TOKEN", "")

# Fallback market data
YFINANCE_ENABLED: bool = _parse_bool(os.getenv("YFINANCE_ENABLED", "true"), True)

# External resources
NSE_NIFTY500_CSV_URL: str = os.getenv(
    "NSE_NIFTY500_CSV_URL",
    "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
)

# ML model storage
MODEL_DIR: Path = PROJECT_ROOT / "services" / "ml-engine" / "models"

# NSE Holidays 2026 (NSE cash market holidays; YYYY-MM-DD)
NSE_HOLIDAYS_2026: Set[str] = {
    "2026-01-26",  # Republic Day
    "2026-03-03",  # Holi
    "2026-03-26",  # Ramzan Id (Eid-ul-Fitr)
    "2026-03-31",  # Mahavir Jayanti
    "2026-04-03",  # Good Friday
    "2026-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-05-28",  # Bakri Id
    "2026-06-26",  # Muharram
    "2026-09-14",  # Ganesh Chaturthi
    "2026-10-02",  # Gandhi Jayanti / Dussehra
    "2026-10-20",  # Diwali Laxmi Pujan (subject to Muhurat timing notices)
    "2026-11-10",  # Diwali Balipratipada
    "2026-11-24",  # Gurunanak Jayanti
    "2026-12-25",  # Christmas
}


def is_market_open() -> bool:
    """Return True only during NSE market hours on non-holiday weekdays."""
    now_ist = datetime.now(MARKET_TZ)

    # Monday=0 ... Sunday=6
    if now_ist.weekday() > 4:
        return False

    holiday_set: Set[str] = NSE_HOLIDAYS_2026 if now_ist.year == 2026 else set()
    if now_ist.date().isoformat() in holiday_set:
        return False

    current_time = now_ist.time()
    return MARKET_OPEN <= current_time <= MARKET_CLOSE


__all__ = [
    "KAFKA_BROKER",
    "KAFKA_TOPIC_RAW",
    "KAFKA_TOPIC_ANOMALIES",
    "DATABASE_URL",
    "OLLAMA_BASE_URL",
    "OLLAMA_MODEL",
    "EXCHANGE",
    "TICKER_SUFFIX",
    "ANOMALY_ZSCORE_THRESHOLD",
    "FORECAST_DAYS",
    "ML_BATCH_SIZE",
    "KITE_API_KEY",
    "KITE_API_SECRET",
    "KITE_ACCESS_TOKEN",
    "UPSTOX_API_KEY",
    "UPSTOX_API_SECRET",
    "UPSTOX_ACCESS_TOKEN",
    "YFINANCE_ENABLED",
    "MODEL_DIR",
    "NSE_NIFTY500_CSV_URL",
    "MARKET_OPEN",
    "MARKET_CLOSE",
    "MARKET_TZ",
    "is_market_open",
]
