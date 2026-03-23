"""Technical indicator calculations for Indian NSE stocks."""

from __future__ import annotations

import logging
from datetime import time as dtime
from typing import Any, Dict, Optional

import pandas as pd
import pandas_ta as ta

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"timestamp", "open", "high", "low", "close", "volume"}


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _prepare_df(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        logger.debug("Input DataFrame is empty.")
        return None

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        logger.debug("Missing required columns: %s", sorted(missing))
        return None

    out = df.copy()
    ts = pd.to_datetime(out["timestamp"], errors="coerce")
    if ts.isna().all():
        logger.debug("All timestamps are invalid.")
        return None

    if ts.dt.tz is None:
        ts = ts.dt.tz_localize("Asia/Kolkata", ambiguous="NaT", nonexistent="shift_forward")
    else:
        ts = ts.dt.tz_convert("Asia/Kolkata")
    out["timestamp"] = ts

    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["timestamp", "close"]).sort_values("timestamp")
    if out.empty:
        return None
    return out


def calculate_rsi(df: pd.DataFrame, period: int = 14) -> Dict[str, Any]:
    data = _prepare_df(df)
    if data is None or len(data) < max(10, period):
        return {"rsi": None, "signal": "neutral", "currency": "INR"}

    rsi_series = ta.rsi(data["close"], length=period)
    rsi_value = _to_float(rsi_series.iloc[-1] if rsi_series is not None and not rsi_series.empty else None)

    signal = "neutral"
    if rsi_value is not None:
        if rsi_value >= 70:
            signal = "overbought"
        elif rsi_value <= 30:
            signal = "oversold"

    return {"rsi": rsi_value, "signal": signal, "currency": "INR"}


def calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, Any]:
    data = _prepare_df(df)
    if data is None or len(data) < max(10, slow + signal):
        return {
            "macd": None,
            "signal_line": None,
            "histogram": None,
            "trend": "bearish",
            "currency": "INR",
        }

    macd_df = ta.macd(data["close"], fast=fast, slow=slow, signal=signal)
    if macd_df is None or macd_df.empty:
        return {
            "macd": None,
            "signal_line": None,
            "histogram": None,
            "trend": "bearish",
            "currency": "INR",
        }

    macd_col = next((c for c in macd_df.columns if c.startswith("MACD_")), None)
    signal_col = next((c for c in macd_df.columns if c.startswith("MACDs_")), None)
    hist_col = next((c for c in macd_df.columns if c.startswith("MACDh_")), None)

    macd_val = _to_float(macd_df[macd_col].iloc[-1] if macd_col else None)
    signal_val = _to_float(macd_df[signal_col].iloc[-1] if signal_col else None)
    hist_val = _to_float(macd_df[hist_col].iloc[-1] if hist_col else None)

    trend = "bullish" if (macd_val is not None and signal_val is not None and macd_val >= signal_val) else "bearish"
    return {
        "macd": macd_val,
        "signal_line": signal_val,
        "histogram": hist_val,
        "trend": trend,
        "currency": "INR",
    }


def calculate_bollinger_bands(df: pd.DataFrame, period: int = 20, std: int = 2) -> Dict[str, Any]:
    data = _prepare_df(df)
    if data is None or len(data) < period:
        return {
            "upper": None,
            "middle": None,
            "lower": None,
            "bandwidth": None,
            "position": "inside",
            "currency": "INR",
        }

    bb_df = ta.bbands(data["close"], length=period, std=std)
    if bb_df is None or bb_df.empty:
        return {
            "upper": None,
            "middle": None,
            "lower": None,
            "bandwidth": None,
            "position": "inside",
            "currency": "INR",
        }

    upper_col = next((c for c in bb_df.columns if c.startswith("BBU_")), None)
    middle_col = next((c for c in bb_df.columns if c.startswith("BBM_")), None)
    lower_col = next((c for c in bb_df.columns if c.startswith("BBL_")), None)

    upper = _to_float(bb_df[upper_col].iloc[-1] if upper_col else None)
    middle = _to_float(bb_df[middle_col].iloc[-1] if middle_col else None)
    lower = _to_float(bb_df[lower_col].iloc[-1] if lower_col else None)
    close = _to_float(data["close"].iloc[-1])

    bandwidth = None
    if upper is not None and lower is not None and middle not in (None, 0):
        bandwidth = (upper - lower) / middle

    position = "inside"
    if close is not None and upper is not None and close > upper:
        position = "above_upper"
    elif close is not None and lower is not None and close < lower:
        position = "below_lower"

    return {
        "upper": upper,
        "middle": middle,
        "lower": lower,
        "bandwidth": bandwidth,
        "position": position,
        "currency": "INR",
    }


def calculate_moving_averages(df: pd.DataFrame) -> Dict[str, Any]:
    data = _prepare_df(df)
    if data is None:
        return {
            "ema_9": None,
            "ema_21": None,
            "sma_50": None,
            "sma_200": None,
            "golden_cross": False,
            "death_cross": False,
            "currency": "INR",
        }

    close = data["close"]
    ema_9 = close.ewm(span=9, adjust=False).mean()
    ema_21 = close.ewm(span=21, adjust=False).mean()
    sma_50 = close.rolling(window=50).mean()
    sma_200 = close.rolling(window=200).mean()

    ema_9_last = _to_float(ema_9.iloc[-1] if not ema_9.empty else None)
    ema_21_last = _to_float(ema_21.iloc[-1] if not ema_21.empty else None)
    sma_50_last = _to_float(sma_50.iloc[-1] if not sma_50.empty else None)
    sma_200_last = _to_float(sma_200.iloc[-1] if not sma_200.empty else None)

    golden_cross = False
    death_cross = False
    if len(data) > 1:
        prev_50 = _to_float(sma_50.iloc[-2])
        prev_200 = _to_float(sma_200.iloc[-2])
        if all(v is not None for v in [prev_50, prev_200, sma_50_last, sma_200_last]):
            golden_cross = prev_50 <= prev_200 and sma_50_last > sma_200_last
            death_cross = prev_50 >= prev_200 and sma_50_last < sma_200_last

    return {
        "ema_9": ema_9_last,
        "ema_21": ema_21_last,
        "sma_50": sma_50_last,
        "sma_200": sma_200_last,
        "golden_cross": golden_cross,
        "death_cross": death_cross,
        "currency": "INR",
    }


def calculate_vwap(df: pd.DataFrame) -> Dict[str, Any]:
    data = _prepare_df(df)
    if data is None:
        return {"vwap": None, "price_vs_vwap": "below", "currency": "INR"}

    latest_date = data["timestamp"].dt.date.max()
    day_df = data[data["timestamp"].dt.date == latest_date].copy()
    day_df = day_df[day_df["timestamp"].dt.time >= dtime(hour=9, minute=15)]
    if day_df.empty:
        day_df = data[data["timestamp"].dt.date == latest_date].copy()
    if day_df.empty:
        return {"vwap": None, "price_vs_vwap": "below", "currency": "INR"}

    typical_price = (day_df["high"] + day_df["low"] + day_df["close"]) / 3.0
    price_volume = typical_price * day_df["volume"].fillna(0)
    cum_volume = day_df["volume"].fillna(0).cumsum()
    vwap_series = price_volume.cumsum() / cum_volume.replace(0, pd.NA)

    vwap = _to_float(vwap_series.iloc[-1] if not vwap_series.empty else None)
    close = _to_float(day_df["close"].iloc[-1] if not day_df.empty else None)
    price_vs_vwap = "above" if (vwap is not None and close is not None and close >= vwap) else "below"
    return {"vwap": vwap, "price_vs_vwap": price_vs_vwap, "currency": "INR"}


def calculate_delivery_percentage(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df is None or "delivery_volume" not in df.columns:
        return None

    data = _prepare_df(df)
    if data is None:
        return {"delivery_pct": None, "signal": "neutral", "currency": "INR"}

    delivery_series = pd.to_numeric(df.get("delivery_volume"), errors="coerce")
    if delivery_series is None or delivery_series.empty:
        return {"delivery_pct": None, "signal": "neutral", "currency": "INR"}

    delivery_volume = _to_float(delivery_series.iloc[-1])
    volume = _to_float(data["volume"].iloc[-1] if not data["volume"].empty else None)
    if delivery_volume is None or volume in (None, 0):
        return {"delivery_pct": None, "signal": "neutral", "currency": "INR"}

    delivery_pct = (delivery_volume / volume) * 100.0
    signal = "neutral"
    if delivery_pct > 50:
        signal = "strong"
    elif delivery_pct < 30:
        signal = "weak"

    return {"delivery_pct": delivery_pct, "signal": signal, "currency": "INR"}


def get_full_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    rsi = calculate_rsi(df)
    macd = calculate_macd(df)
    bollinger = calculate_bollinger_bands(df)
    moving_averages = calculate_moving_averages(df)
    vwap = calculate_vwap(df)
    delivery = calculate_delivery_percentage(df)

    bullish = bearish = 0

    if rsi.get("signal") == "oversold":
        bullish += 1
    elif rsi.get("signal") == "overbought":
        bearish += 1

    if macd.get("trend") == "bullish":
        bullish += 1
    else:
        bearish += 1

    if bollinger.get("position") == "below_lower":
        bullish += 1
    elif bollinger.get("position") == "above_upper":
        bearish += 1

    if moving_averages.get("golden_cross"):
        bullish += 2
    if moving_averages.get("death_cross"):
        bearish += 2

    if vwap.get("price_vs_vwap") == "above":
        bullish += 1
    else:
        bearish += 1

    overall_signal = "hold"
    if bullish >= bearish + 2:
        overall_signal = "buy"
    elif bearish >= bullish + 2:
        overall_signal = "sell"

    result = {
        "rsi": rsi,
        "macd": macd,
        "bollinger_bands": bollinger,
        "moving_averages": moving_averages,
        "vwap": vwap,
        "overall_signal": overall_signal,
        "currency": "INR",
        "exchange": "NSE",
    }
    if delivery is not None:
        result["delivery_percentage"] = delivery

    return result
