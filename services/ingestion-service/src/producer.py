"""Main ingestion entrypoint for Indian NSE stocks.

Priority order:
1) Kite WebSocket (primary)
2) Upstox WebSocket (secondary)
3) yfinance polling fallback
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time as dtime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Ensure project root is importable when this file is run directly.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import shared.constants as constants_module  # noqa: E402
from shared.constants import (  # noqa: E402
    KAFKA_BROKER,
    KAFKA_TOPIC_RAW,
    KITE_ACCESS_TOKEN,
    KITE_API_KEY,
    MARKET_TZ,
    UPSTOX_ACCESS_TOKEN,
    UPSTOX_API_KEY,
    YFINANCE_ENABLED,
    is_market_open,
)
from shared.ticker_registry import TickerRegistry  # noqa: E402

try:
    from kite_websocket_handler import KiteWebSocketHandler
    from upstox_websocket_handler import UpstoxWebSocketHandler
except ImportError:
    from kite_websocket_handler import KiteWebSocketHandler
    from upstox_websocket_handler import UpstoxWebSocketHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


class NSEIngestionPipeline:
    """Coordinates ingestion sources and publishes normalized records to Kafka."""

    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.message_count = 0
        self._count_lock = threading.Lock()
        self._state_lock = threading.Lock()

        self.active_source = "idle"
        self.ticker_registry = TickerRegistry()
        self.tickers = self.ticker_registry.get_all_tickers()

        self.producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=5,
        )

        self.kite_handler: Optional[KiteWebSocketHandler] = None
        self.upstox_handler: Optional[UpstoxWebSocketHandler] = None
        self.ws_supervisor_thread: Optional[threading.Thread] = None
        self.yf_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        logger.info("Loaded %s tickers from registry.", len(self.tickers))
        if not self.tickers:
            logger.warning("Ticker list is empty. Ingestion will run but produce no ticker data.")

        self._activate_best_source()

        self.ws_supervisor_thread = threading.Thread(
            target=self._websocket_supervisor_loop,
            name="ws-supervisor",
            daemon=True,
        )
        self.ws_supervisor_thread.start()

        self.yf_thread = threading.Thread(
            target=self._yfinance_polling_loop,
            name="yfinance-poller",
            daemon=True,
        )
        self.yf_thread.start()

    def shutdown(self) -> None:
        logger.info("Shutting down pipeline...")
        self.stop_event.set()
        self._stop_all_websockets()

        if self.ws_supervisor_thread and self.ws_supervisor_thread.is_alive():
            self.ws_supervisor_thread.join(timeout=5)
        if self.yf_thread and self.yf_thread.is_alive():
            self.yf_thread.join(timeout=5)

        try:
            self.producer.flush(timeout=15)
        except KafkaError as exc:
            logger.warning("Kafka flush warning during shutdown: %s", exc)
        finally:
            self.producer.close()

        logger.info("Pipeline stopped.")

    def heartbeat(self) -> None:
        with self._count_lock:
            count = self.message_count
        with self._state_lock:
            source = self.active_source
        logger.info(
            "Pipeline active | source=%s | tickers=%s | messages_sent=%s",
            source,
            len(self.tickers),
            count,
        )

    def _publish(self, payload: Dict[str, Any]) -> None:
        try:
            future = self.producer.send(KAFKA_TOPIC_RAW, value=payload)
            future.get(timeout=10)
            with self._count_lock:
                self.message_count += 1
        except KafkaError as exc:
            logger.error("Kafka publish failed for %s: %s", payload.get("ticker"), exc)

    def _normalize_message(
        self,
        ticker: str,
        timestamp_value: Any,
        open_value: Any,
        high_value: Any,
        low_value: Any,
        close_value: Any,
        volume_value: Any,
        source: str,
    ) -> Optional[Dict[str, Any]]:
        close_num = self._to_float(close_value)
        if close_num is None:
            return None

        open_num = self._to_float(open_value, close_num)
        high_num = self._to_float(high_value, close_num)
        low_num = self._to_float(low_value, close_num)
        volume_num = self._to_int(volume_value, 0)

        return {
            "ticker": str(ticker).upper().strip(),
            "timestamp": self._to_ist_iso(timestamp_value),
            "open": open_num,
            "high": high_num,
            "low": low_num,
            "close": close_num,
            "volume": volume_num,
            "source": source,
            "exchange": "NSE",
        }

    def _on_kite_tick(self, tick: Dict[str, Any]) -> None:
        message = self._normalize_message(
            ticker=tick.get("ticker", ""),
            timestamp_value=tick.get("timestamp"),
            open_value=tick.get("open"),
            high_value=tick.get("high"),
            low_value=tick.get("low"),
            close_value=tick.get("close") if tick.get("close") is not None else tick.get("last_price"),
            volume_value=tick.get("volume", 0),
            source="kite",
        )
        if message:
            self._publish(message)

    def _on_upstox_tick(self, tick: Dict[str, Any]) -> None:
        message = self._normalize_message(
            ticker=tick.get("ticker", ""),
            timestamp_value=tick.get("timestamp"),
            open_value=tick.get("open"),
            high_value=tick.get("high"),
            low_value=tick.get("low"),
            close_value=tick.get("close") if tick.get("close") is not None else tick.get("last_price"),
            volume_value=tick.get("volume", 0),
            source="upstox",
        )
        if message:
            self._publish(message)

    def _activate_best_source(self) -> None:
        if self._is_ws_window() and self._start_kite_websocket():
            self._set_source("kite")
            logger.info("Active data source: kite")
            return

        if self._is_ws_window() and self._start_upstox_websocket():
            self._set_source("upstox")
            logger.info("Active data source: upstox")
            return

        if YFINANCE_ENABLED:
            self._set_source("yfinance")
            logger.info("Active data source: yfinance")
        else:
            self._set_source("idle")
            logger.warning("No active live source and yfinance fallback is disabled.")

    def _websocket_supervisor_loop(self) -> None:
        while not self.stop_event.wait(30):
            ws_should_run = self._is_ws_window()
            ws_active = self._is_any_websocket_active()

            if not ws_should_run and ws_active:
                logger.info("Market outside websocket window. Disconnecting live feed.")
                self._stop_all_websockets()
                if YFINANCE_ENABLED:
                    self._set_source("yfinance")
                else:
                    self._set_source("idle")
                continue

            if ws_should_run and not ws_active:
                if self._start_kite_websocket():
                    self._set_source("kite")
                    logger.info("Reconnected source at market window: kite")
                    continue

                if self._start_upstox_websocket():
                    self._set_source("upstox")
                    logger.info("Reconnected source at market window: upstox")
                    continue

                if YFINANCE_ENABLED:
                    self._set_source("yfinance")
                else:
                    self._set_source("idle")

    def _start_kite_websocket(self) -> bool:
        if not KITE_ACCESS_TOKEN:
            return False

        try:
            token_resolver = KiteWebSocketHandler(
                api_key=KITE_API_KEY,
                access_token=KITE_ACCESS_TOKEN,
                instrument_tokens={},
                on_tick_callback=self._on_kite_tick,
            )
            token_map = token_resolver.get_instrument_tokens(self.tickers)
            if not token_map:
                logger.warning("Kite token resolution returned no instruments.")
                return False

            self.kite_handler = KiteWebSocketHandler(
                api_key=KITE_API_KEY,
                access_token=KITE_ACCESS_TOKEN,
                instrument_tokens=token_map,
                on_tick_callback=self._on_kite_tick,
            )
            self.kite_handler.run()
            time.sleep(2)
            if self._is_handler_running(self.kite_handler):
                return True
            logger.warning("Kite handler failed to stay alive; falling back.")
            self.kite_handler = None
            return False
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to start Kite websocket: %s", exc)
            self.kite_handler = None
            return False

    def _start_upstox_websocket(self) -> bool:
        if not UPSTOX_ACCESS_TOKEN:
            return False
        if self._is_handler_running(self.kite_handler):
            return False

        try:
            self.upstox_handler = UpstoxWebSocketHandler(
                api_key=UPSTOX_API_KEY,
                access_token=UPSTOX_ACCESS_TOKEN,
                tickers=self.tickers,
                on_tick_callback=self._on_upstox_tick,
            )
            self.upstox_handler.run()
            time.sleep(2)
            if self._is_handler_running(self.upstox_handler):
                return True
            logger.warning("Upstox handler failed to stay alive; falling back.")
            self.upstox_handler = None
            return False
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to start Upstox websocket: %s", exc)
            self.upstox_handler = None
            return False

    def _stop_all_websockets(self) -> None:
        if self.kite_handler is not None:
            self.kite_handler.stop()
            self.kite_handler = None
        if self.upstox_handler is not None:
            self.upstox_handler.stop()
            self.upstox_handler = None

    def _is_any_websocket_active(self) -> bool:
        return self._is_handler_running(self.kite_handler) or self._is_handler_running(self.upstox_handler)

    @staticmethod
    def _is_handler_running(handler: Any) -> bool:
        if handler is None:
            return False
        thread = getattr(handler, "_thread", None)
        return bool(thread and thread.is_alive())

    def _yfinance_polling_loop(self) -> None:
        while not self.stop_event.is_set():
            if self._is_any_websocket_active():
                self.stop_event.wait(5)
                continue

            if not YFINANCE_ENABLED:
                self.stop_event.wait(60)
                continue

            if not is_market_open():
                logger.info("Market closed, sleeping...")
                self.stop_event.wait(60)
                continue

            self._set_source("yfinance")
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {
                    executor.submit(self._fetch_yfinance_ticker, ticker): ticker
                    for ticker in self.tickers
                }
                for future in as_completed(futures):
                    if self.stop_event.is_set():
                        break
                    payload = future.result()
                    if payload:
                        self._publish(payload)

            # Poll every 5 minutes.
            self.stop_event.wait(300)

    def _fetch_yfinance_ticker(self, ticker: str) -> Optional[Dict[str, Any]]:
        try:
            df = yf.download(
                ticker,
                period="1d",
                interval="5m",
                progress=False,
                auto_adjust=False,
                threads=False,
            )
            if df is None or df.empty:
                return None

            row = df.iloc[-1]
            ts = df.index[-1]

            if getattr(ts, "tzinfo", None) is None:
                ts = ts.tz_localize("UTC")

            return self._normalize_message(
                ticker=ticker,
                timestamp_value=ts.to_pydatetime(),
                open_value=row.get("Open"),
                high_value=row.get("High"),
                low_value=row.get("Low"),
                close_value=row.get("Close"),
                volume_value=row.get("Volume"),
                source="yfinance",
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("yfinance fetch failed for %s: %s", ticker, exc)
            return None

    def _is_ws_window(self) -> bool:
        now_ist = datetime.now(MARKET_TZ)
        if not self._is_trading_day(now_ist):
            return False

        current_time = now_ist.time()
        preopen_start = dtime(hour=9, minute=10)
        market_open = dtime(hour=9, minute=15)
        market_close = dtime(hour=15, minute=30)

        in_preopen = preopen_start <= current_time < market_open
        in_market = is_market_open()
        return in_preopen or (in_market and current_time <= market_close)

    @staticmethod
    def _is_trading_day(now_ist: datetime) -> bool:
        if now_ist.weekday() > 4:
            return False

        holiday_set = set()
        if now_ist.year == 2026:
            holiday_set = set(getattr(constants_module, "NSE_HOLIDAYS_2026", set()))
        return now_ist.date().isoformat() not in holiday_set

    def _set_source(self, source: str) -> None:
        with self._state_lock:
            self.active_source = source

    @staticmethod
    def _to_float(value: Any, default: Optional[float] = None) -> Optional[float]:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_int(value: Any, default: int = 0) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_ist_iso(value: Any) -> str:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, (int, float)):
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

        return dt.astimezone(MARKET_TZ).isoformat()


def main() -> None:
    pipeline = NSEIngestionPipeline()

    def _signal_handler(signum: int, _frame: Any) -> None:
        logger.info("Received signal %s. Exiting...", signum)
        pipeline.shutdown()

    signal.signal(signal.SIGINT, _signal_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _signal_handler)

    pipeline.start()

    try:
        while not pipeline.stop_event.wait(300):
            pipeline.heartbeat()
    except KeyboardInterrupt:
        pipeline.shutdown()
    finally:
        if not pipeline.stop_event.is_set():
            pipeline.shutdown()


if __name__ == "__main__":
    main()
