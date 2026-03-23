"""Airflow DAG for monthly NSE ticker universe sync."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from shared.ticker_registry import TickerRegistry

IST = pendulum.timezone("Asia/Kolkata")


def _sync_tickers() -> int:
    return TickerRegistry().sync_to_db()


with DAG(
    dag_id="nse_ticker_sync",
    schedule="0 8 1 * *",
    start_date=datetime(2024, 1, 1, tzinfo=IST),
    catchup=False,
    tags=["data", "nse", "tickers"],
) as dag:
    sync_tickers_task = PythonOperator(
        task_id="sync_nse_tickers",
        python_callable=_sync_tickers,
        retries=3,
        retry_delay=timedelta(minutes=10),
    )
