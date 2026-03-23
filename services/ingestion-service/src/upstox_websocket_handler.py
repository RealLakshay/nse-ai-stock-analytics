"""Upstox WebSocket handler for live NSE ticks.

This handler is intended as a fallback live-stream option when Kite is unavailable.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


# Minimal built-in symbol->ISIN map for common NSE large caps.
# Extend this map or replace with a full instrument master integration for production use.
_KNOWN_ISIN_BY_SYMBOL: Dict[str, str] = {
    "RELIANCE": "INE002A01018",
    "TCS": "INE467B01029",
    "HDFCBANK": "INE040A01034",
    "INFY": "INE009A01021",
    "ICICIBANK": "INE090A01021",
    "HINDUNILVR": "INE030A01027",
    "ITC": "INE154A01025",
    "SBIN": "INE062A01020",
    "BHARTIARTL": "INE397D01024",
    "KOTAKBANK": "INE237A01028",
    "LT": "INE018A01030",
    "AXISBANK": "INE238A01034",
    "ASIANPAINT": "INE021A01026",
    "MARUTI": "INE585B01010",
    "SUNPHARMA": "INE044A01036",
    "TITAN": "INE280A01028",
    "BAJFINANCE": "INE296A01024",
    "WIPRO": "INE075A01022",
    "HCLTECH": "INE860A01027",
    "ULTRACEMCO": "INE481G01011",
}


class UpstoxWebSocketHandler:
    """Manage Upstox v2 market-data feed with reconnect and normalized callbacks."""

    WS_URL = "wss://api.upstox.com/v2/feed/market-data-feed"

    def __init__(
        self,
        api_key: str,
        access_token: str,
        tickers: List[str],
        on_tick_callback: Callable[[Dict], None],
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.access_token = (access_token or "").strip()
        self.tickers = [str(t).strip().upper() for t in (tickers or []) if str(t).strip()]
        self.on_tick_callback = on_tick_callback

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws = None
        self._lock = threading.Lock()

        self._instrument_key_cache: Dict[str, str] = {}
        self._symbol_by_instrument_key: Dict[str, str] = {}
        self._resolve_instrument_keys(self.tickers)

    def connect(self) -> None:
        """Connect and maintain websocket loop (blocking)."""
        if not self.access_token:
            logger.warning(
                "Upstox WebSocket skipped: missing access token. "
                "Fallback source should continue ingestion."
            )
            return
        if not self.api_key:
            logger.warning("Upstox API key is missing. Attempting WebSocket connection anyway.")

        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._connection_loop())
        finally:
            try:
                pending = asyncio.all_tasks(self._loop)
                for task in pending:
                    task.cancel()
                if pending:
                    self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:  # pylint: disable=broad-except
                pass
            self._loop.close()

    def run(self) -> None:
        """Start websocket manager in a background thread."""
        if self._thread and self._thread.is_alive():
            logger.info("Upstox WebSocket handler already running.")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self.connect, name="upstox-ws", daemon=True)
        self._thread.start()
        logger.info("Upstox WebSocket background thread started.")

    def stop(self) -> None:
        """Gracefully stop websocket loop."""
        logger.info("Stopping Upstox WebSocket handler...")
        self._stop_event.set()

        with self._lock:
            if self._loop and self._ws:
                try:
                    asyncio.run_coroutine_threadsafe(self._ws.close(), self._loop)
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning("Error closing Upstox websocket: %s", exc)

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

        logger.info("Upstox WebSocket handler stopped.")

    def map_tickers_to_instrument_keys(self, tickers: List[str]) -> Dict[str, str]:
        """Map .NS tickers to Upstox instrument keys (NSE_EQ|ISIN)."""
        return self._resolve_instrument_keys(tickers)

    async def _connection_loop(self) -> None:
        backoff = 1
        while not self._stop_event.is_set():
            if not self._symbol_by_instrument_key:
                logger.warning("No Upstox instrument keys resolved; skipping websocket connect.")
                return

            try:
                await self._connect_once()
                backoff = 1
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Upstox websocket loop error: %s", exc)

            if self._stop_event.is_set():
                break

            logger.warning("Upstox disconnected. Reconnecting in %ss...", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    async def _connect_once(self) -> None:
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "x-api-key": self.api_key,
        }
        instrument_keys = list(self._symbol_by_instrument_key.keys())
        subscribe_message = {
            "guid": str(uuid.uuid4()),
            "method": "sub",
            "data": {"mode": "full", "instrumentKeys": instrument_keys},
        }

        logger.info("Connecting Upstox WS for %s instrument keys...", len(instrument_keys))
        async with websockets.connect(
            self.WS_URL,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=10_000_000,
        ) as ws:
            with self._lock:
                self._ws = ws

            await ws.send(json.dumps(subscribe_message))
            logger.info("Upstox subscription sent.")

            while not self._stop_event.is_set():
                try:
                    raw_message = await asyncio.wait_for(ws.recv(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                except ConnectionClosed as exc:
                    logger.warning("Upstox websocket closed: %s", exc)
                    break

                self._handle_raw_message(raw_message)

        with self._lock:
            self._ws = None

    def _handle_raw_message(self, raw_message) -> None:
        if isinstance(raw_message, bytes):
            try:
                raw_message = raw_message.decode("utf-8")
            except UnicodeDecodeError:
                logger.debug("Skipping non-text Upstox frame.")
                return

        if not isinstance(raw_message, str):
            return

        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.debug("Skipping non-JSON Upstox message.")
            return

        for normalized in self._extract_normalized_ticks(payload):
            try:
                self.on_tick_callback(normalized)
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Upstox on_tick_callback failed for %s: %s", normalized.get("ticker"), exc)

    def _extract_normalized_ticks(self, payload: Dict) -> List[Dict]:
        """Parse known Upstox payload shapes into normalized ticker ticks."""
        out: List[Dict] = []

        feeds = payload.get("feeds")
        if not isinstance(feeds, dict):
            return out

        for instrument_key, feed_data in feeds.items():
            ticker = self._symbol_by_instrument_key.get(str(instrument_key))
            if not ticker:
                continue

            # Upstox payloads may carry ltpc/ohlc in multiple nested shapes.
            ltpc = self._find_dict(feed_data, ["ltpc", "marketFF.ltpc", "ff.marketFF.ltpc"])
            market_ohlc = self._find_dict(feed_data, ["marketOHLC", "marketFF.marketOHLC", "ff.marketFF.marketOHLC"])

            last_price = self._safe_num((ltpc or {}).get("ltp"))
            timestamp_iso = self._to_iso((ltpc or {}).get("ltt"))

            open_price = high_price = low_price = close_price = None
            if isinstance(market_ohlc, dict):
                ohlc_list = market_ohlc.get("ohlc", [])
                if isinstance(ohlc_list, list) and ohlc_list:
                    day_bar = next(
                        (item for item in ohlc_list if str(item.get("interval", "")).lower() in {"1d", "day"}),
                        ohlc_list[0],
                    )
                    open_price = self._safe_num(day_bar.get("open"))
                    high_price = self._safe_num(day_bar.get("high"))
                    low_price = self._safe_num(day_bar.get("low"))
                    close_price = self._safe_num(day_bar.get("close"))

            if close_price is None:
                close_price = last_price

            normalized = {
                "ticker": ticker,
                "timestamp": timestamp_iso,
                "last_price": last_price,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": self._safe_int((feed_data or {}).get("vtt") or (feed_data or {}).get("volume"), 0),
                "exchange": "NSE",
            }
            out.append(normalized)

        return out

    def _resolve_instrument_keys(self, tickers: List[str]) -> Dict[str, str]:
        mapped: Dict[str, str] = {}
        for ticker in tickers:
            symbol = str(ticker).strip().upper()
            if not symbol:
                continue

            if "|" in symbol and symbol.startswith("NSE_EQ|"):
                instrument_key = symbol
                normalized_ticker = symbol
            else:
                normalized_ticker = symbol if symbol.endswith(".NS") else f"{symbol}.NS"
                base = normalized_ticker[:-3]
                isin = _KNOWN_ISIN_BY_SYMBOL.get(base)
                if not isin:
                    logger.warning("No Upstox instrument key mapping for ticker: %s", normalized_ticker)
                    continue
                instrument_key = f"NSE_EQ|{isin}"

            self._instrument_key_cache[normalized_ticker] = instrument_key
            self._symbol_by_instrument_key[instrument_key] = normalized_ticker
            mapped[normalized_ticker] = instrument_key

        if mapped:
            logger.info("Resolved %s Upstox instrument keys.", len(mapped))
        return mapped

    @staticmethod
    def _find_dict(data: Dict, paths: List[str]) -> Optional[Dict]:
        for path in paths:
            cur = data
            ok = True
            for key in path.split("."):
                if not isinstance(cur, dict) or key not in cur:
                    ok = False
                    break
                cur = cur[key]
            if ok and isinstance(cur, dict):
                return cur
        return None

    @staticmethod
    def _safe_num(value) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value, default: int = 0) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_iso(value) -> str:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, (int, float)):
            # Heuristic: epoch milliseconds for larger values.
            seconds = value / 1000 if value > 10**12 else value
            dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
        elif isinstance(value, str):
            text = value.strip()
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                dt = datetime.now(timezone.utc)
        else:
            dt = datetime.now(timezone.utc)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
