"""Main FastAPI application for the stock AI agent."""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import select

# Ensure project root is importable when this file is run directly.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from shared.constants import OLLAMA_BASE_URL, OLLAMA_MODEL  # noqa: E402
from shared.db_models import Anomaly, get_db  # noqa: E402

try:
    from .rag_retriever import (
        get_comparison_context,
        get_latest_forecast,
        get_price_history,
        get_recent_anomalies,
    )
except ImportError:
    from rag_retriever import (
        get_comparison_context,
        get_latest_forecast,
        get_price_history,
        get_recent_anomalies,
    )


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1)
    ticker: str = Field(..., min_length=1)


class CompareRequest(BaseModel):
    question: str = Field(..., min_length=1)
    ticker_a: str = Field(..., min_length=1)
    ticker_b: str = Field(..., min_length=1)


class ForecastRequest(BaseModel):
    ticker: str = Field(..., min_length=1)


PROMPTS: dict[str, str] = {}
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"


def load_prompts() -> None:
    """Load prompt template text files at startup."""
    required = {
        "history_prompt": "history_prompt.txt",
        "forecast_prompt": "forecast_prompt.txt",
        "anomaly_prompt": "anomaly_prompt.txt",
        "compare_prompt": "compare_prompt.txt",
    }
    loaded: dict[str, str] = {}
    for key, filename in required.items():
        path = PROMPTS_DIR / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing prompt template: {path}")
        loaded[key] = path.read_text(encoding="utf-8")
    PROMPTS.clear()
    PROMPTS.update(loaded)
    logger.info("Loaded %s prompt templates from %s", len(PROMPTS), PROMPTS_DIR)


async def call_ollama(prompt: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=500.0) as client:
            # Try /api/generate first
            response = await client.post(
                f"{OLLAMA_BASE_URL}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                },
            )
            if response.status_code == 200:
                return response.json()["response"]

            # Fallback to /api/chat (newer Ollama versions)
            response = await client.post(
                f"{OLLAMA_BASE_URL}/api/chat",
                json={
                    "model": OLLAMA_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False,
                },
            )
            if response.status_code == 200:
                return response.json()["message"]["content"]

            raise HTTPException(status_code=502, detail=f"Ollama error: {response.text}")

    except httpx.ConnectError:
        raise HTTPException(
            status_code=502,
            detail="Cannot connect to Ollama. Make sure Ollama is running.",
        ) from None


def _format_with_guard(template: str, values: dict[str, Any], prompt_name: str) -> str:
    try:
        return template.format(**values)
    except KeyError as exc:
        missing = str(exc).strip("'")
        raise HTTPException(
            status_code=500,
            detail=f"Prompt template '{prompt_name}' missing variable: {missing}",
        ) from exc


def _extract_latest_price(history_text: str) -> str:
    lines = [line.strip() for line in history_text.splitlines() if line.strip()]
    if len(lines) < 2:
        return "N/A"
    parts = [part.strip() for part in lines[-1].split("|")]
    if len(parts) < 2:
        return "N/A"
    return parts[1]


def _extract_latest_anomaly_fields(anomaly_text: str) -> dict[str, str]:
    if anomaly_text.strip().lower().startswith("no anomalies"):
        return {
            "anomaly_type": "none",
            "price": "N/A",
            "zscore": "N/A",
            "timestamp": "N/A",
        }

    lines = [line.strip() for line in anomaly_text.splitlines() if line.strip()]
    if len(lines) < 2:
        return {
            "anomaly_type": "unknown",
            "price": "N/A",
            "zscore": "N/A",
            "timestamp": "N/A",
        }

    latest = lines[1]
    date_part = latest.split(":", 1)[0].strip() if ":" in latest else "N/A"
    anomaly_type = "unknown"
    price = "N/A"
    zscore = "N/A"

    parts = [part.strip() for part in latest.split("|")]
    if parts:
        first = parts[0]
        if ":" in first:
            anomaly_type = first.split(":", 1)[1].strip()
    for part in parts[1:]:
        lower = part.lower()
        if lower.startswith("price="):
            price = part.split("=", 1)[1].strip()
        if lower.startswith("zscore="):
            zscore = part.split("=", 1)[1].strip()

    return {
        "anomaly_type": anomaly_type,
        "price": price,
        "zscore": zscore,
        "timestamp": date_part,
    }


app = FastAPI(title="Stock AI Agent", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event() -> None:
    try:
        load_prompts()
        logger.info("AI agent service startup complete.")
    except Exception:
        logger.exception("Failed during application startup")
        raise


@app.post("/ask")
async def ask(request: AskRequest) -> dict[str, Any]:
    try:
        question = request.question.strip()
        ticker = request.ticker.strip().upper()
        from rag_retriever import fetch_and_store_if_missing

        fetch_and_store_if_missing(request.ticker)
        if not question or not ticker:
            raise HTTPException(status_code=400, detail="question and ticker are required")

        price_history = get_price_history(ticker)
        forecast_summary = get_latest_forecast(ticker)
        anomalies_summary = get_recent_anomalies(ticker)

        q_lower = question.lower()
        context_used: list[str] = []

        if "forecast" in q_lower or "predict" in q_lower:
            template = PROMPTS["forecast_prompt"]
            context_used = ["price_history", "forecast"]
            prompt = _format_with_guard(
                template,
                {
                    "ticker": ticker,
                    "current_price": _extract_latest_price(price_history),
                    "forecast_data": forecast_summary,
                    "question": question,
                },
                "forecast_prompt",
            )
        elif "spike" in q_lower or "crash" in q_lower or "anomaly" in q_lower:
            template = PROMPTS["anomaly_prompt"]
            latest_anomaly = _extract_latest_anomaly_fields(anomalies_summary)
            context_used = ["anomalies", "price_history"]
            prompt = _format_with_guard(
                template,
                {
                    "ticker": ticker,
                    "anomaly_type": latest_anomaly["anomaly_type"],
                    "price": latest_anomaly["price"],
                    "zscore": latest_anomaly["zscore"],
                    "timestamp": latest_anomaly["timestamp"],
                    "context_prices": price_history,
                },
                "anomaly_prompt",
            )
        else:
            template = PROMPTS["history_prompt"]
            context_used = ["price_history", "forecast", "anomalies"]
            prompt = _format_with_guard(
                template,
                {
                    "ticker": ticker,
                    "question": question,
                    "price_data": (
                        f"{price_history}\n\nForecast Snapshot:\n{forecast_summary}\n\n"
                        f"Recent Anomalies:\n{anomalies_summary}"
                    ),
                },
                "history_prompt",
            )

        answer = await call_ollama(prompt)
        return {
            "ticker": ticker,
            "question": question,
            "answer": answer,
            "context_used": context_used,
        }
    except HTTPException:
        raise
    except httpx.HTTPError as exc:
        logger.exception("Ollama request failed in /ask")
        raise HTTPException(status_code=502, detail=f"Ollama error: {exc}") from exc
    except Exception as exc:
        logger.exception("Unexpected error in /ask")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/forecast")
async def forecast(request: ForecastRequest) -> dict[str, Any]:
    try:
        from rag_retriever import fetch_and_store_if_missing

        fetch_and_store_if_missing(request.ticker)
        ticker = request.ticker.strip().upper()
        if not ticker:
            raise HTTPException(status_code=400, detail="ticker is required")

        forecast_summary = get_latest_forecast(ticker)
        history = get_price_history(ticker, days=7)
        prompt = _format_with_guard(
            PROMPTS["forecast_prompt"],
            {
                "ticker": ticker,
                "current_price": _extract_latest_price(history),
                "forecast_data": forecast_summary,
                "question": "Provide a concise forecast explanation.",
            },
            "forecast_prompt",
        )

        analyst_note = await call_ollama(prompt)
        return {
            "ticker": ticker,
            "forecast_summary": forecast_summary,
            "analyst_note": analyst_note,
        }
    except HTTPException:
        raise
    except httpx.HTTPError as exc:
        logger.exception("Ollama request failed in /forecast")
        raise HTTPException(status_code=502, detail=f"Ollama error: {exc}") from exc
    except Exception as exc:
        logger.exception("Unexpected error in /forecast")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/compare")
async def compare(request: CompareRequest) -> dict[str, Any]:
    try:
        from rag_retriever import fetch_and_store_if_missing

        fetch_and_store_if_missing(request.ticker_a)
        fetch_and_store_if_missing(request.ticker_b)
        ticker_a = request.ticker_a.strip().upper()
        ticker_b = request.ticker_b.strip().upper()
        question = request.question.strip()
        if not ticker_a or not ticker_b or not question:
            raise HTTPException(status_code=400, detail="question, ticker_a, ticker_b are required")

        data_a, data_b = get_comparison_context(ticker_a, ticker_b)
        prompt = _format_with_guard(
            PROMPTS["compare_prompt"],
            {
                "ticker_a": ticker_a,
                "ticker_b": ticker_b,
                "data_a": data_a,
                "data_b": data_b,
                "question": question,
            },
            "compare_prompt",
        )
        comparison = await call_ollama(prompt)
        return {
            "ticker_a": ticker_a,
            "ticker_b": ticker_b,
            "question": question,
            "comparison": comparison,
        }
    except HTTPException:
        raise
    except httpx.HTTPError as exc:
        logger.exception("Ollama request failed in /compare")
        raise HTTPException(status_code=502, detail=f"Ollama error: {exc}") from exc
    except Exception as exc:
        logger.exception("Unexpected error in /compare")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/anomalies/{ticker}")
async def anomalies(ticker: str) -> dict[str, Any]:
    symbol = ticker.strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="ticker is required")

    cutoff = datetime.now(timezone.utc) - timedelta(days=30)

    db_gen = None
    try:
        db_gen = get_db()
        db = next(db_gen)
        stmt = (
            select(Anomaly)
            .where(Anomaly.ticker == symbol, Anomaly.timestamp >= cutoff)
            .order_by(Anomaly.timestamp.desc())
        )
        rows = db.execute(stmt).scalars().all()

        payload = []
        for row in rows:
            payload.append(
                {
                    "id": row.id,
                    "ticker": row.ticker,
                    "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                    "close": float(row.close) if isinstance(row.close, Decimal) else row.close,
                    "zscore": float(row.zscore) if isinstance(row.zscore, Decimal) else row.zscore,
                    "anomaly_type": row.anomaly_type,
                    "alert_sent": bool(row.alert_sent) if row.alert_sent is not None else False,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                }
            )

        return {"ticker": symbol, "days": 30, "count": len(payload), "anomalies": payload}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to fetch anomalies for %s", symbol)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        if db_gen is not None:
            db_gen.close()


@app.get("/health")
async def health() -> dict[str, Any]:
    ollama_connected = False
    db_connected = False

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{OLLAMA_BASE_URL.rstrip('/')}/api/tags")
            ollama_connected = response.status_code == 200
    except Exception:
        ollama_connected = False

    db_gen = None
    try:
        db_gen = get_db()
        db = next(db_gen)
        db.execute(select(1))
        db_connected = True
    except Exception:
        db_connected = False
    finally:
        if db_gen is not None:
            db_gen.close()

    return {
        "status": "ok",
        "ollama_connected": ollama_connected,
        "db_connected": db_connected,
    }
