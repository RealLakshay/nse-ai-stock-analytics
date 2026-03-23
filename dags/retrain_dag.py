"""Airflow DAG for weekly Prophet model retraining."""

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
TRAIN_SCRIPT_PATH = PROJECT_ROOT / "services" / "ml-engine" / "train.py"


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


def _run_retraining() -> None:
    train_all_tickers = _load_callable_from_file(TRAIN_SCRIPT_PATH, "train_all_tickers")
    train_all_tickers()


with DAG(
    dag_id="weekly_model_retraining",
    schedule="0 6 * * 0",
    start_date=datetime(2024, 1, 1, tzinfo=IST),
    catchup=False,
    tags=["ml", "training", "nse"],
) as dag:
    retrain_task = PythonOperator(
        task_id="train_prophet_models",
        python_callable=_run_retraining,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
