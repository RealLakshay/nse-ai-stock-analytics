"""Alerting helpers for anomaly events."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from typing import Any

import httpx


logger = logging.getLogger(__name__)


class AlertManager:
    """Sends anomaly alerts to logs and an optional webhook."""

    def __init__(self, webhook_url: str | None = None, timeout_seconds: float = 10.0) -> None:
        env_url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
        self.webhook_url = (webhook_url or env_url or "").strip() or None
        self.timeout_seconds = timeout_seconds

    async def _post_webhook(self, payload: dict[str, Any]) -> None:
        if not self.webhook_url:
            return
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(self.webhook_url, json=payload)
            response.raise_for_status()
        logger.info("Webhook alert sent for %s", payload.get("ticker", "UNKNOWN"))

    def _post_webhook_in_background(self, payload: dict[str, Any]) -> None:
        """Dispatch webhook call on a daemon thread so stream processing is not blocked."""

        def _runner() -> None:
            try:
                asyncio.run(self._post_webhook(payload))
            except Exception:
                logger.exception("Failed to send webhook alert")

        threading.Thread(target=_runner, daemon=True, name="alert-webhook").start()

    def send_alert(self, anomaly: dict[str, Any]) -> None:
        """Log anomaly and optionally post it to a webhook."""
        try:
            ticker = anomaly.get("ticker", "UNKNOWN")
            anomaly_type = anomaly.get("anomaly_type", "unknown")
            close = anomaly.get("close")
            zscore = anomaly.get("zscore")
            timestamp = anomaly.get("timestamp")

            logger.warning(
                "\U0001F6A8 ANOMALY: %s %s | price=%s zscore=%s",
                ticker,
                anomaly_type,
                close,
                zscore,
            )

            if not self.webhook_url:
                return

            payload = {
                "ticker": ticker,
                "anomaly_type": anomaly_type,
                "close": close,
                "zscore": zscore,
                "timestamp": timestamp,
                "message": "Unusual price movement detected",
            }
            self._post_webhook_in_background(payload)
        except Exception:
            logger.exception("Unexpected error while handling send_alert")

    def send_bulk_summary(self, anomalies: list[dict[str, Any]]) -> None:
        """Log one-line summary of anomaly batch."""
        try:
            if not anomalies:
                logger.info("Batch summary: 0 anomalies")
                return

            labels = [
                f"{item.get('ticker', 'UNKNOWN')}({item.get('anomaly_type', 'unknown')})"
                for item in anomalies
            ]
            logger.info("Batch summary: %d anomalies \u2014 %s", len(anomalies), ", ".join(labels))
        except Exception:
            logger.exception("Unexpected error while handling send_bulk_summary")
