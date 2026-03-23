"""Kafka consumer that processes stock prices and detects anomalies."""

from __future__ import annotations

import json
import logging
import signal
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

# Ensure project root is importable when this file is run directly.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from shared.constants import KAFKA_BROKER, KAFKA_TOPIC_ANOMALIES, KAFKA_TOPIC_RAW  # noqa: E402
from shared.db_models import Anomaly, StockPrice, get_db  # noqa: E402

try:
    from .alert_manager import AlertManager
    from .anomaly_detector import AnomalyDetector
except ImportError:
    from alert_manager import AlertManager
    from anomaly_detector import AnomalyDetector


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
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


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round4(value: Any) -> float | None:
    number = _to_float(value)
    if number is None:
        return None
    return round(number, 4)


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        # Handle both seconds and milliseconds epoch timestamps.
        seconds = float(value) / 1000.0 if float(value) > 10**12 else float(value)
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def clean_message(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Validate and normalize incoming payload for storage and anomaly checks."""
    ticker = str(payload.get("ticker", "")).strip().upper()
    if not ticker:
        logger.debug("Skipping message with missing ticker: %s", payload)
        return None

    timestamp = _parse_timestamp(payload.get("timestamp"))
    if timestamp is None:
        logger.debug("Skipping %s due to invalid timestamp: %r", ticker, payload.get("timestamp"))
        return None

    close = _round4(payload.get("close"))
    if close is None or close <= 0:
        logger.debug("Skipping %s due to invalid close: %r", ticker, payload.get("close"))
        return None

    open_price = _round4(payload.get("open"))
    high = _round4(payload.get("high"))
    low = _round4(payload.get("low"))
    volume_rounded = _round4(payload.get("volume"))
    volume = int(volume_rounded) if volume_rounded is not None else None

    return {
        "ticker": ticker,
        "timestamp": timestamp,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "source": payload.get("source"),
        "exchange": payload.get("exchange"),
    }


def persist_stock_price(db: Session, cleaned: dict[str, Any]) -> None:
    stmt = pg_insert(StockPrice.__table__).values(**cleaned)
    stmt = stmt.on_conflict_do_nothing(index_elements=["ticker", "timestamp"])
    db.execute(stmt)


def persist_anomaly(db: Session, anomaly: dict[str, Any]) -> None:
    timestamp = _parse_timestamp(anomaly.get("timestamp"))
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    stmt = pg_insert(Anomaly.__table__).values(
        ticker=anomaly.get("ticker"),
        timestamp=timestamp,
        close=anomaly.get("close"),
        zscore=anomaly.get("zscore"),
        anomaly_type=anomaly.get("anomaly_type"),
        alert_sent=False,
    )
    db.execute(stmt)


def publish_anomaly(producer: KafkaProducer, anomaly: dict[str, Any]) -> None:
    try:
        future = producer.send(KAFKA_TOPIC_ANOMALIES, value=anomaly)
        future.get(timeout=10)
    except KafkaError:
        logger.exception("Failed to publish anomaly to topic %s", KAFKA_TOPIC_ANOMALIES)


def main() -> None:
    detector = AnomalyDetector()
    alert_manager = AlertManager()
    processed_count = 0

    consumer = KafkaConsumer(
        KAFKA_TOPIC_RAW,
        bootstrap_servers=[KAFKA_BROKER],
        group_id="processing-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        enable_auto_commit=True,
    )
    anomaly_producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
        acks="all",
        retries=3,
    )

    shutdown_requested = False

    def _handle_signal(signum: int, _frame: Any) -> None:
        nonlocal shutdown_requested
        shutdown_requested = True
        logger.info("Received signal %s. Shutting down stream processor...", signum)

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    logger.info(
        "Processing service started. broker=%s topic=%s group_id=processing-group",
        KAFKA_BROKER,
        KAFKA_TOPIC_RAW,
    )

    try:
        for message in consumer:
            if shutdown_requested:
                break

            try:
                payload = message.value
                if not isinstance(payload, dict):
                    logger.debug("Skipping non-dict payload at offset %s", message.offset)
                    continue

                cleaned = clean_message(payload)
                if cleaned is None:
                    continue

                anomaly: dict[str, Any] | None = None
                with db_session() as db:
                    persist_stock_price(db, cleaned)

                    anomaly = detector.detect(
                        cleaned["ticker"],
                        cleaned["close"],
                        cleaned["timestamp"],
                    )
                    if anomaly:
                        persist_anomaly(db, anomaly)

                    db.commit()

                if anomaly:
                    publish_anomaly(anomaly_producer, anomaly)
                    alert_manager.send_alert(anomaly)

                processed_count += 1
                if processed_count % 100 == 0:
                    logger.info(
                        "Processed %s messages. Last: %s @ %s",
                        processed_count,
                        cleaned["ticker"],
                        cleaned["close"],
                    )
            except Exception:  # pylint: disable=broad-except
                logger.exception("Error processing message at offset %s", message.offset)
                continue
    finally:
        try:
            anomaly_producer.flush(timeout=10)
        except Exception:
            logger.exception("Error flushing anomaly producer during shutdown")
        finally:
            anomaly_producer.close()
            consumer.close()

        logger.info("Processing service stopped.")


if __name__ == "__main__":
    main()
