# Stock Analysis LLM Platform

A microservices-based stock analytics platform focused on NSE (India) equities.  
It ingests live/near-live market data, detects anomalies, trains and runs forecasts, and exposes an AI assistant API for analysis.

## What This Project Does

- Ingests stock prices from Kite/Upstox (with yfinance fallback).
- Streams data through Kafka-compatible Redpanda.
- Stores prices, anomalies, and forecasts in Postgres.
- Detects price anomalies in real time.
- Trains Prophet models and generates forward forecasts.
- Uses Ollama-hosted LLM prompts for Q&A, forecasting commentary, and ticker comparison.
- Schedules recurring jobs with Airflow.

## Architecture

Services defined in `docker-compose.yml`:

- `postgres`: primary relational datastore.
- `redpanda`: Kafka-compatible message broker.
- `airflow`: scheduling for ticker sync, retraining, and forecast inference.
- `ingestion-service`: market feed producer (`stock.prices.raw`).
- `processing-service`: consumer + anomaly detection + DB persistence.
- `ai-agent-service`: FastAPI service for ask/forecast/compare/anomalies endpoints.
- `analytics-service`: technical and portfolio analytics utilities.

Data flow:

1. `ingestion-service` publishes raw price ticks to `stock.prices.raw`.
2. `processing-service` consumes raw ticks, stores them in `stock_prices`, and flags anomalies.
3. Detected anomalies are stored in `anomalies` and published to `stock.anomalies`.
4. `ml-engine/train.py` trains Prophet models and stores artifacts.
5. `ml-engine/inference.py` writes forecasts to `forecasts`.
6. `ai-agent-service` builds context from Postgres and calls Ollama for responses.

## Repository Layout

```text
.
|- services/
|  |- ingestion-service/src/
|  |- processing-service/src/
|  |- ai-agent-service/
|  |- ml-engine/
|  `- analytics-service/src/
|- shared/
|- dags/
|- postgres/migrations/
|- docker-compose.yml
|- requirements.txt
`- backfill.py
```

## Prerequisites

- Docker + Docker Compose
- Python 3.10+ (for local/non-container runs)
- Optional broker credentials:
  - Kite (`KITE_API_KEY`, `KITE_ACCESS_TOKEN`)
  - Upstox (`UPSTOX_API_KEY`, `UPSTOX_ACCESS_TOKEN`)
- Ollama running locally or remotely (not started by this compose file)

## Quick Start (Docker)

1. Create and edit `.env` in the project root.
2. Start core stack:

```bash
docker compose up --build -d
```

3. Apply SQL migrations to Postgres.

PowerShell:

```powershell
$files = Get-ChildItem .\postgres\migrations\*.sql | Sort-Object Name
foreach ($file in $files) {
  Get-Content $file.FullName -Raw | docker exec -i postgres psql -U stockuser -d stockdb
}
```

4. (Optional but recommended) sync NSE tickers once:

```powershell
docker exec -it ingestion-service python -c "from shared.ticker_registry import TickerRegistry; print(TickerRegistry().sync_to_db())"
```

5. Check health:

```bash
curl http://localhost:8000/health
```

## AI Agent API

Base URL: `http://localhost:8000`

- `GET /health`
- `POST /ask`
- `POST /forecast`
- `POST /compare`
- `GET /anomalies/{ticker}`

Example requests:

```bash
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d "{\"ticker\":\"RELIANCE.NS\",\"question\":\"What is the short-term outlook?\"}"
```

```bash
curl -X POST http://localhost:8000/compare \
  -H "Content-Type: application/json" \
  -d "{\"ticker_a\":\"TCS.NS\",\"ticker_b\":\"INFY.NS\",\"question\":\"Which looks stronger this week?\"}"
```

```bash
curl http://localhost:8000/anomalies/RELIANCE.NS
```

## Airflow DAGs

Airflow UI: `http://localhost:8080`

DAGs in `dags/`:

- `nse_ticker_sync`: monthly ticker sync.
- `weekly_model_retraining`: weekly Prophet retraining.
- `daily_price_forecast`: weekday forecast generation.

## Local Development (Without Docker Compose)

Install dependencies:

```bash
pip install -r requirements.txt
```

Run core scripts manually:

- Ingestion: `python services/ingestion-service/src/producer.py`
- Processing: `python services/processing-service/src/stream_processor.py`
- AI API: `uvicorn main:app --reload --port 8000 --app-dir services/ai-agent-service`
- Train models: `python services/ml-engine/train.py`
- Run inference: `python services/ml-engine/inference.py`

Note: You still need a running Postgres, broker, and Ollama endpoint.

## Environment Variables

Common variables (see `shared/constants.py`):

- `DATABASE_URL` (default: `postgresql://stockuser:stockpass@localhost:5432/stockdb`)
- `KAFKA_BROKER` (default: `localhost:19092`)
- `KAFKA_TOPIC_RAW` (default: `stock.prices.raw`)
- `KAFKA_TOPIC_ANOMALIES` (default: `stock.anomalies`)
- `OLLAMA_BASE_URL` (default: `http://localhost:11434`)
- `OLLAMA_MODEL` (default: `mistral`)
- `FORECAST_DAYS` (default: `7`)
- `ML_BATCH_SIZE` (default: `50`)
- `ANOMALY_ZSCORE_THRESHOLD` (default: `3.0`)
- `KITE_API_KEY`, `KITE_API_SECRET`, `KITE_ACCESS_TOKEN`
- `UPSTOX_API_KEY`, `UPSTOX_API_SECRET`, `UPSTOX_ACCESS_TOKEN`
- `ALERT_WEBHOOK_URL` (optional)
- `YFINANCE_ENABLED` (default: `true`)

## Backfill Helper

`backfill.py` provides a quick way to push recent yfinance history into Kafka via the ingestion container.

## Troubleshooting

- `health` shows `ollama_connected=false`: start Ollama and ensure `OLLAMA_BASE_URL` is reachable.
- No ingested ticks: verify broker at `KAFKA_BROKER` and at least one data source (Kite/Upstox/yfinance).
- Empty forecasts: run training first (`train.py`) so model files exist in `services/ml-engine/models`.
- Empty AI responses: confirm prompt files exist under `services/ai-agent-service/prompts/`.
