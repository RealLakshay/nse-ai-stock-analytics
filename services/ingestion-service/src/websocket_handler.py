"""Finnhub WebSocket connection manager with auto-reconnect support."""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Callable, Dict, List, Optional

import websocket

logger = logging.getLogger(__name__)


class FinnhubWebSocketHandler:
    """Manage Finnhub trade stream lifecycle and message parsing."""

    def __init__(
        self,
        api_key: str,
        tickers: List[str],
        on_message_callback: Callable[[Dict], None],
    ) -> None:
        self.api_key = api_key
        self.tickers = [ticker.strip().upper() for ticker in tickers if ticker.strip()]
        self.on_message_callback = on_message_callback

        self.ws_url = f"wss://ws.finnhub.io?token={self.api_key}"
        self.ws_app: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()
        self._lock = threading.Lock()

    def connect(self) -> None:
        """Open connection and subscribe to configured ticker symbols."""
        if not self.api_key:
            raise ValueError("Finnhub API key is required for websocket connection.")

        logger.info("Connecting to Finnhub WebSocket: %s", self.ws_url)
        self.ws_app = websocket.WebSocketApp(
            self.ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_ping=self._on_ping,
            on_pong=self._on_pong,
        )

        # ping_interval/ping_timeout keep heartbeat alive and detect stale sockets.
        self.ws_app.run_forever(ping_interval=20, ping_timeout=10)

    def run(self) -> None:
        """Start connection loop in background thread with exponential reconnect backoff."""
        if self._thread and self._thread.is_alive():
            logger.info("Finnhub websocket handler is already running.")
            return

        self._stop_event.clear()

        def _runner() -> None:
            backoff_seconds = 1
            while not self._stop_event.is_set():
                self._connected_event.clear()
                try:
                    self.connect()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.exception("WebSocket connection crashed: %s", exc)

                if self._connected_event.is_set():
                    # A successful connect happened before disconnect; reset backoff.
                    backoff_seconds = 1

                if self._stop_event.is_set():
                    break

                logger.warning(
                    "WebSocket disconnected. Reconnecting in %ss...",
                    backoff_seconds,
                )
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 60)

        self._thread = threading.Thread(target=_runner, name="finnhub-ws", daemon=True)
        self._thread.start()
        logger.info("Finnhub websocket background thread started.")

    def stop(self) -> None:
        """Gracefully stop reconnect loop and close websocket."""
        logger.info("Stopping Finnhub websocket handler...")
        self._stop_event.set()

        with self._lock:
            if self.ws_app is not None:
                try:
                    self.ws_app.close()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning("Error while closing websocket: %s", exc)

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

        logger.info("Finnhub websocket handler stopped.")

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        logger.info("Connected to Finnhub WebSocket.")
        self._connected_event.set()

        for ticker in self.tickers:
            payload = {"type": "subscribe", "symbol": ticker}
            try:
                ws.send(json.dumps(payload))
                logger.info("Subscribed to ticker: %s", ticker)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Failed subscribing %s: %s", ticker, exc)

    def _on_message(self, _: websocket.WebSocketApp, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            logger.warning("Received non-JSON message: %s", message)
            return

        msg_type = payload.get("type")
        if msg_type == "ping":
            logger.debug("Received ping message payload: %s", payload)
            return

        if msg_type != "trade":
            logger.debug("Ignoring non-trade message: %s", payload)
            return

        trades = payload.get("data", [])
        if not isinstance(trades, list):
            logger.warning("Malformed trade payload: %s", payload)
            return

        for trade in trades:
            try:
                parsed = {
                    "ticker": str(trade["s"]),
                    "price": float(trade["p"]),
                    "timestamp": int(trade["t"]),
                    "volume": float(trade.get("v", 0)),
                }
                self.on_message_callback(parsed)
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning("Skipping malformed trade entry %s: %s", trade, exc)

    def _on_error(self, _: websocket.WebSocketApp, error: object) -> None:
        logger.error("Finnhub websocket error: %s", error)

    def _on_close(
        self,
        _: websocket.WebSocketApp,
        close_status_code: Optional[int],
        close_msg: Optional[str],
    ) -> None:
        logger.warning(
            "Finnhub websocket closed. code=%s msg=%s",
            close_status_code,
            close_msg,
        )
        self._connected_event.clear()

    def _on_ping(self, ws: websocket.WebSocketApp, message: bytes) -> None:
        logger.debug("Received ping frame; sending pong.")
        try:
            ws.send(message, opcode=websocket.ABNF.OPCODE_PONG)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to send pong: %s", exc)

    def _on_pong(self, _: websocket.WebSocketApp, __: bytes) -> None:
        logger.debug("Received pong frame from server.")
