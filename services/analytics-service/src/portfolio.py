"""Portfolio math utilities for Indian equity portfolios (INR)."""

from __future__ import annotations

import logging
from typing import Dict

import numpy as np
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

TRADING_DAYS_PER_YEAR = 252


def _normalize_prices(prices: Dict[str, pd.Series]) -> Dict[str, pd.Series]:
    normalized: Dict[str, pd.Series] = {}
    for ticker, series in (prices or {}).items():
        if series is None:
            continue
        s = pd.Series(series).copy()
        s = pd.to_numeric(s, errors="coerce")
        s = s.dropna()
        if s.empty:
            continue
        s.index = pd.to_datetime(s.index, errors="coerce")
        s = s[~s.index.isna()]
        s = s.sort_index()
        if not s.empty:
            normalized[str(ticker)] = s
    return normalized


def calculate_returns(prices: Dict[str, pd.Series]) -> pd.DataFrame:
    """Daily percentage returns per ticker."""
    normalized = _normalize_prices(prices)
    if not normalized:
        return pd.DataFrame()

    price_df = pd.concat(normalized, axis=1)
    returns = price_df.pct_change().dropna(how="all")
    return returns


def calculate_sharpe_ratio(prices: Dict[str, pd.Series], risk_free_rate: float = 0.067) -> Dict[str, float]:
    """Annualized Sharpe ratio per ticker using Indian risk-free rate default."""
    returns = calculate_returns(prices)
    if returns.empty:
        return {}

    daily_rfr = risk_free_rate / TRADING_DAYS_PER_YEAR
    result: Dict[str, float] = {}
    for ticker in returns.columns:
        series = returns[ticker].dropna()
        if len(series) < 2:
            result[ticker] = np.nan
            continue

        mean_daily_return = float(series.mean())
        std_daily_return = float(series.std(ddof=1))
        if np.isclose(std_daily_return, 0.0):
            result[ticker] = np.nan
            continue

        sharpe = (mean_daily_return - daily_rfr) / std_daily_return * np.sqrt(TRADING_DAYS_PER_YEAR)
        result[ticker] = float(sharpe)
    return result


def calculate_max_drawdown(prices: Dict[str, pd.Series]) -> Dict[str, float]:
    """Maximum drawdown percentage per ticker (negative value)."""
    normalized = _normalize_prices(prices)
    output: Dict[str, float] = {}

    for ticker, series in normalized.items():
        running_max = series.cummax()
        drawdown = (series / running_max) - 1.0
        if drawdown.empty:
            output[ticker] = np.nan
            continue
        output[ticker] = float(drawdown.min() * 100.0)
    return output


def _fetch_market_series(market_ticker: str) -> pd.Series:
    df = yf.download(market_ticker, period="2y", interval="1d", progress=False, auto_adjust=False)
    if df is None or df.empty:
        return pd.Series(dtype=float)

    series = pd.to_numeric(df["Close"], errors="coerce").dropna()
    series.index = pd.to_datetime(series.index, errors="coerce")
    series = series[~series.index.isna()].sort_index()
    return series


def calculate_beta(prices: Dict[str, pd.Series], market_ticker: str = "^NSEI") -> Dict[str, float]:
    """Beta of each ticker against Nifty benchmark."""
    normalized = _normalize_prices(prices)
    if market_ticker not in normalized:
        market_series = _fetch_market_series(market_ticker)
        if not market_series.empty:
            normalized[market_ticker] = market_series
        else:
            logger.warning("Unable to fetch market benchmark %s for beta calculation.", market_ticker)
            return {ticker: np.nan for ticker in normalized.keys() if ticker != market_ticker}

    returns = calculate_returns(normalized)
    if returns.empty or market_ticker not in returns.columns:
        return {ticker: np.nan for ticker in normalized.keys() if ticker != market_ticker}

    market_returns = returns[market_ticker].dropna()
    market_var = float(market_returns.var(ddof=1)) if len(market_returns) > 1 else np.nan
    if np.isnan(market_var) or np.isclose(market_var, 0.0):
        return {ticker: np.nan for ticker in normalized.keys() if ticker != market_ticker}

    beta: Dict[str, float] = {}
    for ticker in returns.columns:
        if ticker == market_ticker:
            continue

        pair = pd.concat([returns[ticker], market_returns], axis=1, join="inner").dropna()
        if len(pair) < 2:
            beta[ticker] = np.nan
            continue

        cov = float(np.cov(pair.iloc[:, 0], pair.iloc[:, 1], ddof=1)[0, 1])
        beta[ticker] = cov / market_var

    return beta


def calculate_correlation_matrix(prices: Dict[str, pd.Series]) -> pd.DataFrame:
    """Correlation matrix of daily returns."""
    returns = calculate_returns(prices)
    if returns.empty:
        return pd.DataFrame()
    return returns.corr()


def calculate_nse_specific_metrics(prices: Dict[str, pd.Series]) -> Dict[str, Dict[str, float]]:
    """Compute 52-week high/low and distance from 52-week high."""
    normalized = _normalize_prices(prices)
    output: Dict[str, Dict[str, float]] = {}

    for ticker, series in normalized.items():
        if series.empty:
            output[ticker] = {
                "52w_high": np.nan,
                "52w_low": np.nan,
                "distance_from_52w_high_pct": np.nan,
            }
            continue

        trailing = series.tail(252)
        high_52 = float(trailing.max())
        low_52 = float(trailing.min())
        latest = float(trailing.iloc[-1])

        distance = np.nan
        if not np.isclose(high_52, 0.0):
            distance = ((high_52 - latest) / high_52) * 100.0

        output[ticker] = {
            "52w_high": high_52,
            "52w_low": low_52,
            "distance_from_52w_high_pct": float(distance) if not np.isnan(distance) else np.nan,
        }
    return output


def portfolio_summary(prices: Dict[str, pd.Series]) -> Dict[str, Dict[str, float]]:
    """Combine core portfolio metrics per ticker."""
    normalized = _normalize_prices(prices)
    if not normalized:
        return {}

    sharpe = calculate_sharpe_ratio(normalized)
    max_dd = calculate_max_drawdown(normalized)
    beta = calculate_beta(normalized, market_ticker="^NSEI")
    nse_metrics = calculate_nse_specific_metrics(normalized)

    summary: Dict[str, Dict[str, float]] = {}
    for ticker, series in normalized.items():
        if ticker == "^NSEI":
            continue

        latest_price = float(series.iloc[-1]) if not series.empty else np.nan
        if len(series) >= 31:
            past_price = float(series.iloc[-31])
            ret_30d = ((latest_price / past_price) - 1.0) * 100.0 if not np.isclose(past_price, 0.0) else np.nan
        else:
            ret_30d = np.nan

        t_metrics = nse_metrics.get(
            ticker,
            {"52w_high": np.nan, "52w_low": np.nan, "distance_from_52w_high_pct": np.nan},
        )
        summary[ticker] = {
            "sharpe": float(sharpe.get(ticker, np.nan)),
            "max_drawdown": float(max_dd.get(ticker, np.nan)),
            "beta_vs_nifty": float(beta.get(ticker, np.nan)),
            "latest_price_inr": latest_price,
            "30d_return_pct": float(ret_30d) if not np.isnan(ret_30d) else np.nan,
            "52w_high": float(t_metrics.get("52w_high", np.nan)),
            "distance_from_52w_high_pct": float(t_metrics.get("distance_from_52w_high_pct", np.nan)),
        }

    return summary
