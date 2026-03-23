"""Airflow DAG for weekday NSE daily price forecasting."""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

IST = pendulum.timezone("Asia/Kolkata")
INFERENCE_SCRIPT_PATH = PROJECT_ROOT / "services" / "ml-engine" / "inference.py"


def _load_callable_from_file(file_path: Path, function_name: str):
    spec = importlib.util.spec_from_file_location(f"airflow_{function_name}", str(file_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fn = getattr(module, function_name, None)
    if fn is None:
        raise AttributeError(f"Function {function_name} not found in {file_path}")
    return fn


def _run_forecast() -> None:
    run_inference_for_all = _load_callable_from_file(INFERENCE_SCRIPT_PATH, "run_inference_for_all")
    run_inference_for_all()


with DAG(
    dag_id="daily_price_forecast",
    schedule="0 7 * * 1-5",
    start_date=datetime(2024, 1, 1, tzinfo=IST),
    catchup=False,
    tags=["ml", "forecast", "nse"],
) as dag:
    forecast_task = PythonOperator(
        task_id="run_daily_forecasts",
        python_callable=_run_forecast,
        retries=1,
        retry_delay=timedelta(minutes=10),
    )
