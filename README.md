# NSE AI Stock Analytics Platform

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.109-green)
![Kafka](https://img.shields.io/badge/Kafka-Redpanda-red)
![LLM](https://img.shields.io/badge/LLM-Mistral-purple)

A production-grade real-time stock market data pipeline and AI analytics platform built exclusively for the Indian NSE market. Ask questions about any NSE stock in plain English and get analyst-style answers powered by a local LLM running on your machine.

---

## What This Project Does

- Streams live price data for **500+ NSE Nifty stocks** via Zerodha Kite WebSocket / Upstox / yfinance fallback
- Processes and cleans tick data through **Kafka (Redpanda)** with second-level latency
- Detects unusual price movements in real time using **Z-score + IQR anomaly detection** including NSE 20% circuit breaker simulation
- Trains **Facebook Prophet** time series models weekly and generates 7-day price forecasts every morning before market open
- Answers natural language questions about any NSE stock using **Ollama Mistral LLM** with RAG (Retrieval Augmented Generation)
- Compares two stocks side by side with Nifty 50 benchmark
- Explains why a stock spiked or crashed using anomaly context
- All prices in **Indian Rupees (₹)**, fully IST timezone aware, NSE holiday calendar included
- Fully containerized with **Docker** — one command to run everything

---

## Architecture
```
Zerodha Kite WebSocket / Upstox / yfinance
              ↓
     Kafka (Redpanda) — stock.prices.raw
              ↓
     Processing Service — clean + anomaly detect
              ↓
         PostgreSQL
        ↙          ↘
Prophet ML        AI Agent (FastAPI)
Forecasts              ↓
        ↘         Ollama Mistral LLM
         ↘             ↓
          → Natural Language Answers
```

---

## Services

| Service | Description |
|---------|-------------|
| `postgres` | Primary relational datastore for prices, anomalies, forecasts |
| `redpanda` | Kafka-compatible message broker |
| `airflow` | Scheduler for ticker sync, model retraining, daily forecasts |
| `ingestion-service` | Market feed producer → Kafka |
| `processing-service` | Kafka consumer + anomaly detection + DB persistence |
| `ai-agent-service` | FastAPI LLM agent — ask, forecast, compare endpoints |
| `analytics-service` | Technical indicators (RSI, MACD, Bollinger Bands) + portfolio math |
| `ml-engine` | Prophet model training and inference scripts |

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.10 |
| API Framework | FastAPI + Uvicorn |
| Message Broker | Redpanda (Kafka-compatible) |
| Database | PostgreSQL 15 + SQLAlchemy |
| ML / Forecasting | Facebook Prophet, scikit-learn |
| LLM | Ollama (Mistral 7B) — runs locally |
| Live Data | Zerodha Kite WebSocket, Upstox WebSocket |
| Fallback Data | yfinance (Yahoo Finance) |
| Orchestration | Apache Airflow 2.8 |
| Containerization | Docker + Docker Compose |
| Market | NSE India — 500+ Nifty stocks |

---

## Project Structure
```
.
├── services/
│   ├── ingestion-service/src/
│   │   ├── producer.py
│   │   ├── kite_websocket_handler.py
│   │   └── upstox_websocket_handler.py
│   ├── processing-service/src/
│   │   ├── stream_processor.py
│   │   ├── anomaly_detector.py
│   │   └── alert_manager.py
│   ├── ai-agent-service/
│   │   ├── main.py
│   │   ├── rag_retriever.py
│   │   └── prompts/
│   ├── ml-engine/
│   │   ├── train.py
│   │   ├── inference.py
│   │   └── models/
│   └── analytics-service/src/
│       ├── calculations.py
│       └── portfolio.py
├── shared/
│   ├── constants.py
│   ├── db_models.py
│   └── ticker_registry.py
├── dags/
│   ├── sync_tickers_dag.py
│   ├── retrain_dag.py
│   └── forecast_dag.py
├── postgres/migrations/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env
```

---

## Prerequisites

- **Docker Desktop** running
- **Python 3.10+**
- **Ollama** installed locally with Mistral pulled
- Optional: Zerodha demat account for live Kite WebSocket data
- Optional: Upstox account as alternative live feed

---

## Quick Start

### 1 — Clone the repository
```bash
git clone https://github.com/RealLakshay/nse-ai-stock-analytics.git
cd nse-ai-stock-analytics
```

### 2 — Configure environment
```bash
cp .env.example .env
# Edit .env with your API keys
```

### 3 — Pull Mistral model
```bash
ollama pull mistral
```

### 4 — Start the full stack
```bash
docker compose up -d
```

### 5 — Run database migrations
```powershell
# PowerShell
$files = Get-ChildItem .\postgres\migrations\*.sql | Sort-Object Name
foreach ($file in $files) {
  Get-Content $file.FullName -Raw | docker exec -i postgres psql -U stockuser -d stockdb
}
```

### 6 — Sync NSE tickers
```bash
docker exec ingestion-service python -c "
from shared.ticker_registry import TickerRegistry
print(TickerRegistry().sync_to_db(), 'tickers synced')
"
```

### 7 — Test the API
```bash
curl http://localhost:8000/health
```

---

## AI Agent API

Base URL: `http://localhost:8000`

API docs: `http://localhost:8000/docs`

### Ask about any stock
```bash
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{
    "ticker": "RELIANCE.NS",
    "question": "What is the price trend for Reliance this month?"
  }'
```

### Get price forecast
```bash
curl -X POST http://localhost:8000/forecast \
  -H "Content-Type: application/json" \
  -d '{"ticker": "TCS.NS"}'
```

### Compare two stocks
```bash
curl -X POST http://localhost:8000/compare \
  -H "Content-Type: application/json" \
  -d '{
    "ticker_a": "TCS.NS",
    "ticker_b": "INFY.NS",
    "question": "Which looks stronger this week?"
  }'
```

### Get anomalies
```bash
curl http://localhost:8000/anomalies/RELIANCE.NS
```

### Market status
```bash
curl http://localhost:8000/market/status
```

---

## Airflow Dashboards

| Dashboard | URL |
|-----------|-----|
| Airflow UI | http://localhost:8080 |
| API Docs | http://localhost:8000/docs |
| Redpanda Console | http://localhost:9644 |

Airflow login: `admin / admin`

### DAGs

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `nse_ticker_sync` | Monthly | Sync Nifty 500 list from NSE |
| `weekly_model_retraining` | Every Sunday 6 AM IST | Retrain Prophet models |
| `daily_price_forecast` | Weekdays 7 AM IST | Generate forecasts before market open |

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://stockuser:stockpass@postgres:5432/stockdb` | Postgres connection |
| `KAFKA_BROKER` | `redpanda:19092` | Kafka broker address |
| `OLLAMA_BASE_URL` | `http://host.docker.internal:11434` | Ollama endpoint |
| `OLLAMA_MODEL` | `mistral` | LLM model name |
| `FORECAST_DAYS` | `7` | Days to forecast ahead |
| `ANOMALY_ZSCORE_THRESHOLD` | `3.0` | Z-score threshold for anomaly detection |
| `KITE_API_KEY` | — | Zerodha Kite API key |
| `KITE_ACCESS_TOKEN` | — | Zerodha daily access token |
| `UPSTOX_API_KEY` | — | Upstox API key |
| `YFINANCE_ENABLED` | `true` | Enable yfinance fallback |

---

## Data Sources

| Source | Type | Coverage |
|--------|------|----------|
| Zerodha Kite WebSocket | Live tick-by-tick | All NSE stocks |
| Upstox WebSocket | Live tick-by-tick | All NSE stocks |
| yfinance | Historical + polling | All NSE stocks (free) |

Data source priority: Kite → Upstox → yfinance

---

## Troubleshooting

**Ollama not connected**
```bash
ollama serve  # make sure Ollama is running
```

**No data in responses**
```bash
# Check if stock_prices table has data
docker exec -it postgres psql -U stockuser -d stockdb -c "SELECT COUNT(*) FROM stock_prices;"
```

**Ingestion service crashing**
```bash
docker compose logs ingestion-service
# Check KAFKA_BROKER=redpanda:19092 in .env (not localhost)
```

**Postgres not starting**
```bash
docker compose down -v  # clear old volume
docker compose up postgres -d
```

---

## Local Development Without Docker
```bash
pip install -r requirements.txt

# Run services individually
python services/ingestion-service/src/producer.py
python services/processing-service/src/stream_processor.py
uvicorn main:app --reload --port 8000 --app-dir services/ai-agent-service
python services/ml-engine/train.py
python services/ml-engine/inference.py
```

Note: Requires running Postgres, Redpanda, and Ollama locally.

---

## Roadmap

- [ ] Frontend dashboard with real-time charts
- [ ] Multi-agent architecture for complex queries
- [ ] Sentiment analysis from NSE announcements
- [ ] Portfolio tracker with P&L calculations
- [ ] Mobile alerts for anomaly detection
- [ ] Support for BSE stocks

---

## License

MIT License — free to use, modify, and distribute.

---

Built with ❤️ for the Indian stock market