"""Zerodha Kite WebSocket handler for live NSE tick ingestion.

Note:
- Kite `access_token` is short-lived and must be generated daily via the Kite login flow.
- Alternatively, generate it programmatically using `kite.generate_session(request_token, api_secret=...)`.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from kiteconnect import KiteConnect, KiteTicker

logger = logging.getLogger(__name__)


class KiteWebSocketHandler:
    """Manage KiteTicker connection lifecycle and normalize full-mode ticks."""

    def __init__(
        self,
        api_key: str,
        access_token: str,
        instrument_tokens: Dict[str, int],
        on_tick_callback: Callable[[Dict], None],
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.access_token = (access_token or "").strip()
        self.on_tick_callback = on_tick_callback

        self.instrument_tokens: Dict[str, int] = {
            str(symbol).strip().upper(): int(token)
            for symbol, token in (instrument_tokens or {}).items()
            if str(symbol).strip()
        }
        self._token_to_symbol: Dict[int, str] = {
            token: symbol for symbol, token in self.instrument_tokens.items()
        }

        self._kws: Optional[KiteTicker] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # Symbol->token cache to avoid repeated `instruments("NSE")` calls.
        self._instrument_cache: Dict[str, int] = dict(self.instrument_tokens)
        self._instrument_master_loaded = False

    def connect(self) -> None:
        """Initialize KiteTicker, wire callbacks, subscribe tokens, and connect."""
        if not self.api_key or not self.access_token:
            logger.warning(
                "Kite WebSocket skipped: missing api_key/access_token. "
                "Fallback data source should handle ingestion."
            )
            return

        if not self._token_to_symbol:
            logger.warning("Kite WebSocket skipped: no instrument tokens available.")
            return

        logger.info("Connecting Kite WebSocket for %s instrument(s)...", len(self._token_to_symbol))

        try:
            self._kws = KiteTicker(
                api_key=self.api_key,
                access_token=self.access_token,
                reconnect=True,
                reconnect_max_tries=300,
                reconnect_max_delay=60,
            )
            self._kws.on_ticks = self._on_ticks
            self._kws.on_connect = self._on_connect
            self._kws.on_close = self._on_close
            self._kws.on_error = self._on_error

            # Blocking connect; `run()` starts this in a background thread.
            self._kws.connect(threaded=False)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                "Kite WebSocket connection failed (%s). "
                "Likely invalid/expired access token; skipping without crash.",
                exc,
            )

    def run(self) -> None:
        """Start websocket in a background thread (Kite auto-reconnect handles disconnects)."""
        if self._thread and self._thread.is_alive():
            logger.info("Kite WebSocket handler is already running.")
            return

        self._stop_event.clear()

        def _runner() -> None:
            self.connect()

        self._thread = threading.Thread(target=_runner, name="kite-ws", daemon=True)
        self._thread.start()
        logger.info("Kite WebSocket background thread started.")

    def stop(self) -> None:
        """Gracefully stop websocket and background thread."""
        logger.info("Stopping Kite WebSocket handler...")
        self._stop_event.set()

        with self._lock:
            if self._kws is not None:
                try:
                    self._kws.close()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning("Error closing Kite WebSocket: %s", exc)

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

        logger.info("Kite WebSocket handler stopped.")

    def get_instrument_tokens(self, symbols: List[str]) -> Dict[str, int]:
        """Resolve symbols to instrument tokens from Kite instrument master with caching."""
        requested = [str(symbol).strip().upper() for symbol in (symbols or []) if str(symbol).strip()]
        if not requested:
            return {}

        missing = [symbol for symbol in requested if symbol not in self._instrument_cache]
        if missing and not self._instrument_master_loaded:
            if not self.api_key:
                logger.warning("Cannot fetch instrument tokens: KITE_API_KEY is missing.")
                return {sym: self._instrument_cache[sym] for sym in requested if sym in self._instrument_cache}

            try:
                kite = KiteConnect(api_key=self.api_key)
                if self.access_token:
                    kite.set_access_token(self.access_token)

                instruments = kite.instruments("NSE")
                master_map = {
                    str(item.get("tradingsymbol", "")).upper(): int(item["instrument_token"])
                    for item in instruments
                    if item.get("tradingsymbol") and item.get("instrument_token") is not None
                }

                for symbol in missing:
                    base_symbol = symbol[:-3] if symbol.endswith(".NS") else symbol
                    token = master_map.get(base_symbol)
                    if token is not None:
                        self._instrument_cache[symbol] = token

                self._instrument_master_loaded = True
                logger.info(
                    "Instrument token cache loaded. Requested=%s resolved=%s",
                    len(requested),
                    len([sym for sym in requested if sym in self._instrument_cache]),
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Failed to fetch Kite instruments('NSE'): %s", exc)

        resolved = {sym: self._instrument_cache[sym] for sym in requested if sym in self._instrument_cache}
        if not resolved:
            logger.warning("No instrument tokens resolved for requested symbols.")
        return resolved

    def _on_connect(self, ws, _response) -> None:
        tokens = list(self._token_to_symbol.keys())
        if not tokens:
            logger.warning("No tokens available on connect; nothing to subscribe.")
            return

        try:
            ws.subscribe(tokens)
            ws.set_mode(ws.MODE_FULL, tokens)
            logger.info("Subscribed to %s instrument tokens in FULL mode.", len(tokens))
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Failed to subscribe/set mode on Kite WebSocket: %s", exc)

    def _on_ticks(self, _ws, ticks: List[Dict]) -> None:
        for tick in ticks or []:
            try:
                instrument_token = int(tick.get("instrument_token"))
            except (TypeError, ValueError):
                logger.debug("Skipping tick with invalid instrument_token: %s", tick)
                continue

            symbol = self._token_to_symbol.get(instrument_token)
            if not symbol:
                logger.debug("Skipping tick for unknown token=%s", instrument_token)
                continue

            ohlc = tick.get("ohlc") or {}
            timestamp = tick.get("exchange_timestamp") or tick.get("last_trade_time")
            timestamp_iso = self._to_iso(timestamp)

            normalized = {
                "ticker": symbol,
                "timestamp": timestamp_iso,
                "last_price": tick.get("last_price"),
                "open": ohlc.get("open"),
                "high": ohlc.get("high"),
                "low": ohlc.get("low"),
                "close": ohlc.get("close"),
                "volume": tick.get("volume_traded", 0),
                "exchange": "NSE",
            }

            try:
                self.on_tick_callback(normalized)
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("on_tick_callback failed for %s: %s", symbol, exc)

    def _on_close(self, _ws, code, reason) -> None:
        if self._stop_event.is_set():
            logger.info("Kite WebSocket closed gracefully. code=%s reason=%s", code, reason)
            return
        logger.warning("Kite WebSocket disconnected. code=%s reason=%s", code, reason)

    def _on_error(self, _ws, code, reason) -> None:
        logger.error("Kite WebSocket error. code=%s reason=%s", code, reason)
        reason_text = str(reason).lower()
        if "token" in reason_text or "authentication" in reason_text:
            logger.warning(
                "Kite access token appears invalid/expired. "
                "Skipping live Kite stream until token refresh."
            )

    @staticmethod
    def _to_iso(value) -> str:
        if isinstance(value, datetime):
            dt = value
        else:
            dt = datetime.now(timezone.utc)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc).isoformat()
