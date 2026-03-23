"""Anomaly detection for streaming stock prices."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from math import sqrt
from typing import Any


logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Detects price anomalies using rolling Z-score and IQR checks."""

    def __init__(self, zscore_threshold: float = 3.0, window: int = 20) -> None:
        self.zscore_threshold = float(zscore_threshold)
        self.window = int(window)
        self._buffers: defaultdict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=self.window)
        )
        logger.info(
            "AnomalyDetector initialized (zscore_threshold=%s, window=%s)",
            self.zscore_threshold,
            self.window,
        )

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float:
        """Compute percentile using linear interpolation."""
        if not values:
            return 0.0
        if len(values) == 1:
            return values[0]

        ordered = sorted(values)
        rank = (len(ordered) - 1) * percentile
        low = int(rank)
        high = low + 1
        if high >= len(ordered):
            return ordered[low]
        weight = rank - low
        return ordered[low] * (1 - weight) + ordered[high] * weight

    def detect(self, ticker: str, close_price: float, timestamp: Any) -> dict[str, Any] | None:
        """Return anomaly dict when detected, otherwise None."""
        try:
            price = float(close_price)
        except (TypeError, ValueError):
            logger.warning("Skipping non-numeric close price for %s: %r", ticker, close_price)
            return None

        buffer = self._buffers[ticker]
        buffer.append(price)

        if len(buffer) < 10:
            return None

        prices = list(buffer)
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_dev = sqrt(variance)
        zscore = (price - mean_price) / std_dev if std_dev > 0 else 0.0

        q1 = self._percentile(prices, 0.25)
        q3 = self._percentile(prices, 0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        iqr_outlier = price < lower_bound or price > upper_bound

        if zscore > self.zscore_threshold or iqr_outlier:
            anomaly_type = "spike" if price > mean_price else "drop"
            ts_value = timestamp.isoformat() if hasattr(timestamp, "isoformat") else str(timestamp)
            anomaly = {
                "ticker": ticker,
                "timestamp": ts_value,
                "close": price,
                "zscore": round(zscore, 4),
                "anomaly_type": anomaly_type,
                "is_anomaly": True,
            }
            logger.warning(
                "Anomaly detected for %s | type=%s price=%s zscore=%.4f",
                ticker,
                anomaly_type,
                price,
                zscore,
            )
            return anomaly

        return None

    def reset(self, ticker: str) -> None:
        """Clear rolling buffer for one ticker."""
        if ticker in self._buffers:
            self._buffers[ticker].clear()
            logger.info("Reset rolling buffer for %s", ticker)
