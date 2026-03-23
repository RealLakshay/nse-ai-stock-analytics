"""Dynamic ticker registry for NSE symbols."""

from __future__ import annotations

import io
import logging
from typing import List, Sequence, Set

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from shared.constants import NSE_NIFTY500_CSV_URL, TICKER_SUFFIX
from shared.db_models import SessionLocal

logger = logging.getLogger(__name__)


# Nifty 50 fallback universe (50 symbols).
FALLBACK_NIFTY50: List[str] = [
    "RELIANCE.NS",
    "TCS.NS",
    "HDFCBANK.NS",
    "INFY.NS",
    "ICICIBANK.NS",
    "HINDUNILVR.NS",
    "ITC.NS",
    "SBIN.NS",
    "BHARTIARTL.NS",
    "KOTAKBANK.NS",
    "LT.NS",
    "AXISBANK.NS",
    "ASIANPAINT.NS",
    "MARUTI.NS",
    "SUNPHARMA.NS",
    "TITAN.NS",
    "BAJFINANCE.NS",
    "WIPRO.NS",
    "HCLTECH.NS",
    "ULTRACEMCO.NS",
    "NTPC.NS",
    "POWERGRID.NS",
    "NESTLEIND.NS",
    "TATAMOTORS.NS",
    "BAJAJFINSV.NS",
    "M&M.NS",
    "TECHM.NS",
    "ONGC.NS",
    "JSWSTEEL.NS",
    "ADANIPORTS.NS",
    "INDUSINDBK.NS",
    "HDFCLIFE.NS",
    "SBILIFE.NS",
    "GRASIM.NS",
    "TATASTEEL.NS",
    "CIPLA.NS",
    "COALINDIA.NS",
    "DRREDDY.NS",
    "EICHERMOT.NS",
    "HEROMOTOCO.NS",
    "BRITANNIA.NS",
    "SHRIRAMFIN.NS",
    "APOLLOHOSP.NS",
    "DIVISLAB.NS",
    "UPL.NS",
    "ADANIENT.NS",
    "HINDALCO.NS",
    "BPCL.NS",
    "TATACONSUM.NS",
    "BAJAJ-AUTO.NS",
]

# Hardcoded Nifty 100 set for ML-focused subset filtering.
NIFTY_100_BASE_SYMBOLS: Set[str] = {
    "RELIANCE",
    "TCS",
    "HDFCBANK",
    "INFY",
    "ICICIBANK",
    "HINDUNILVR",
    "ITC",
    "SBIN",
    "BHARTIARTL",
    "KOTAKBANK",
    "LT",
    "AXISBANK",
    "ASIANPAINT",
    "MARUTI",
    "SUNPHARMA",
    "TITAN",
    "BAJFINANCE",
    "WIPRO",
    "HCLTECH",
    "ULTRACEMCO",
    "NTPC",
    "POWERGRID",
    "NESTLEIND",
    "TATAMOTORS",
    "BAJAJFINSV",
    "M&M",
    "TECHM",
    "ONGC",
    "JSWSTEEL",
    "ADANIPORTS",
    "INDUSINDBK",
    "HDFCLIFE",
    "SBILIFE",
    "GRASIM",
    "TATASTEEL",
    "CIPLA",
    "COALINDIA",
    "DRREDDY",
    "EICHERMOT",
    "HEROMOTOCO",
    "BRITANNIA",
    "SHRIRAMFIN",
    "APOLLOHOSP",
    "DIVISLAB",
    "UPL",
    "ADANIENT",
    "HINDALCO",
    "BPCL",
    "TATACONSUM",
    "BAJAJ-AUTO",
    "ADANIGREEN",
    "AMBUJACEM",
    "ABB",
    "BANKBARODA",
    "BEL",
    "BOSCHLTD",
    "CHOLAFIN",
    "COLPAL",
    "DABUR",
    "DLF",
    "GAIL",
    "GODREJCP",
    "HAVELLS",
    "HINDPETRO",
    "ICICIPRULI",
    "INDIGO",
    "INDUSTOWER",
    "IOC",
    "JINDALSTEL",
    "LICI",
    "LUPIN",
    "MCDOWELL-N",
    "NAUKRI",
    "PIDILITIND",
    "PFC",
    "PNB",
    "RECLTD",
    "SIEMENS",
    "SRF",
    "TVSMOTOR",
    "TRENT",
    "VEDL",
    "ZYDUSLIFE",
    "PAGEIND",
    "MOTHERSON",
    "TORNTPHARM",
    "ADANIPOWER",
    "CANBK",
    "CGPOWER",
    "CONCOR",
    "CUMMINSIND",
    "ICICIGI",
    "IRCTC",
    "LODHA",
    "MRF",
    "OBEROIRLTY",
    "POLYCAB",
    "SAIL",
    "UNIONBANK",
    "VOLTAS",
}


class TickerRegistry:
    """Downloads, stores, and serves NSE ticker universes."""

    def __init__(self) -> None:
        self.csv_url = NSE_NIFTY500_CSV_URL
        self.suffix = TICKER_SUFFIX or ".NS"

    def fetch_from_nse(self) -> List[str]:
        """Fetch Nifty universe from NSE CSV; fallback to hardcoded Nifty 50."""
        logger.info("Fetching NSE tickers from CSV: %s", self.csv_url)
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            ),
            "Accept": "text/csv,*/*",
        }

        try:
            response = requests.get(self.csv_url, headers=headers, timeout=20)
            response.raise_for_status()
            df = pd.read_csv(io.StringIO(response.text))

            symbol_column = self._resolve_symbol_column(df.columns)
            if symbol_column is None:
                raise ValueError("CSV does not contain a 'Symbol' column.")

            symbols = (
                df[symbol_column]
                .dropna()
                .astype(str)
                .str.strip()
                .str.upper()
                .tolist()
            )
            tickers = self._to_yfinance_tickers(symbols)
            if not tickers:
                raise ValueError("No symbols parsed from NSE CSV.")

            logger.info("Fetched %s tickers from NSE CSV.", len(tickers))
            return tickers
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("NSE fetch failed (%s). Using fallback Nifty 50 list.", exc)
            return FALLBACK_NIFTY50.copy()

    def sync_to_db(self) -> int:
        """Upsert ticker universe into Postgres and return synced count."""
        tickers = self.fetch_from_nse()
        if not tickers:
            logger.warning("No tickers available for DB sync.")
            return 0

        session = SessionLocal()
        try:
            session.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS tickers (
                        id BIGSERIAL PRIMARY KEY,
                        ticker VARCHAR(32) NOT NULL UNIQUE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )

            upsert_stmt = text(
                """
                INSERT INTO tickers (ticker, updated_at)
                VALUES (:ticker, NOW())
                ON CONFLICT (ticker)
                DO UPDATE SET updated_at = NOW()
                """
            )
            session.execute(upsert_stmt, [{"ticker": ticker} for ticker in tickers])
            session.commit()
            logger.info("Synced %s tickers to Postgres.", len(tickers))
            return len(tickers)
        except SQLAlchemyError as exc:
            session.rollback()
            logger.exception("Ticker sync to DB failed: %s", exc)
            return 0
        finally:
            session.close()

    def get_all_tickers(self) -> List[str]:
        """Load tickers from DB; fallback to NSE fetch when table/data missing."""
        session = SessionLocal()
        try:
            session.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS tickers (
                        id BIGSERIAL PRIMARY KEY,
                        ticker VARCHAR(32) NOT NULL UNIQUE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )
            session.commit()

            rows = session.execute(text("SELECT ticker FROM tickers ORDER BY ticker")).fetchall()
            tickers = [str(row[0]) for row in rows if row and row[0]]
            if tickers:
                logger.info("Loaded %s tickers from Postgres.", len(tickers))
                return tickers

            logger.info("Tickers table is empty. Falling back to NSE fetch.")
            return self.fetch_from_nse()
        except SQLAlchemyError as exc:
            session.rollback()
            logger.warning("DB read failed for tickers (%s). Falling back to NSE fetch.", exc)
            return self.fetch_from_nse()
        finally:
            session.close()

    def get_tickers_for_ml(self) -> List[str]:
        """Return Nifty 100 subset for ML workflows."""
        all_tickers = self.get_all_tickers()
        nifty100_with_suffix = {f"{symbol}{self.suffix}" for symbol in NIFTY_100_BASE_SYMBOLS}
        filtered = [ticker for ticker in all_tickers if ticker in nifty100_with_suffix]

        if filtered:
            logger.info("Selected %s Nifty-100 tickers for ML.", len(filtered))
            return sorted(set(filtered))

        logger.warning("No overlap found in DB tickers; returning hardcoded Nifty-100 subset.")
        return sorted(nifty100_with_suffix)

    def _to_yfinance_tickers(self, symbols: Sequence[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for symbol in symbols:
            base = symbol.strip().upper()
            if not base:
                continue
            ticker = base if base.endswith(self.suffix.upper()) else f"{base}{self.suffix}"
            if ticker not in seen:
                seen.add(ticker)
                out.append(ticker)
        return out

    @staticmethod
    def _resolve_symbol_column(columns: Sequence[str]) -> str | None:
        for column in columns:
            if str(column).strip().lower() == "symbol":
                return str(column)
        return None
