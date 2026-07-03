# Bitcoin Market Intelligence Pipeline

[![CI](https://github.com/myuyen0304/Bitcoin-Market-Intelligence-Pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/myuyen0304/Bitcoin-Market-Intelligence-Pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![dbt](https://img.shields.io/badge/dbt-DuckDB-FF694B.svg)

Local-first Data Engineering portfolio project for Bitcoin market analytics.

## What This Project Shows

This repo builds a reproducible pipeline from public APIs to analytics-ready marts:

```text
CoinGecko / Binance / Fear & Greed
        -> Bronze Parquet
        -> dbt + DuckDB Silver/Gold marts
        -> Streamlit dashboard
```

It is intentionally local-first for the MVP. AWS, Terraform, Kafka, and production Airflow remain roadmap items after the recruiter demo is stable.

## Business Questions

- How is BTC price trending by day, MA7, MA30, and 30-day volatility?
- Does Fear & Greed sentiment move with daily BTC returns?
- Which days have unusual trading volume?
- How fresh is the data currently powering the dashboard?

## Repository Map

```text
bitcoin_pipeline/
  ingestion/               Python API fetchers and shared utilities
  data/bronze/             Local Hive-partitioned Bronze Parquet outputs
  dbt/models/staging/      Silver cleanup models
  dbt/models/intermediate/ Daily enrichment joins
  dbt/models/marts/        Gold business marts
  dashboard/app.py         Streamlit dashboard reading Gold marts only
  dags/                    Existing Airflow daily ingestion DAG
  target.md                Architecture, schema, and progress source of truth
```

## Setup

Use Python 3.11+.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

For dbt, point profiles to this repo before running commands:

```powershell
$env:DBT_PROFILES_DIR=(Get-Location).Path
```

## Run The Local Demo

Generate fresh Bronze Parquet:

```powershell
.\.venv\Scripts\python.exe bitcoin_pipeline\run_local_bronze_ingestion.py
```

Build Silver and Gold marts in DuckDB:

```powershell
$env:DBT_PROFILES_DIR=(Get-Location).Path
dbt run
dbt test
```

Open the dashboard:

```powershell
streamlit run bitcoin_pipeline/dashboard/app.py
```

Dashboard URL: `http://localhost:8501`

## Docker Dashboard

After Bronze and dbt have produced `bitcoin_pipeline/data/bitcoin_pipeline.duckdb`, the dashboard can also run in Docker:

```powershell
docker compose up --build dashboard
```

## Orchestration with Airflow (Docker)

Run the whole pipeline — 3 parallel ingestions -> Bronze validation -> `dbt build`
— on a local Airflow (LocalExecutor + Postgres) instead of running each step by hand:

```powershell
docker compose -f docker-compose.airflow.yml build
docker compose -f docker-compose.airflow.yml up airflow-init   # one-off: metadata DB + admin user
docker compose -f docker-compose.airflow.yml up -d
```

Open the Airflow UI at `http://localhost:8080` (login `admin` / `admin`), enable the
`btc_daily_ingestion` DAG, and trigger it. Tear down with:

```powershell
docker compose -f docker-compose.airflow.yml down
```

Notes:

- dbt runs in an isolated venv inside the image, so its dependencies never conflict
  with Airflow's own pinned packages.
- Bronze Parquet and the DuckDB database are written under `bitcoin_pipeline/data/`,
  the same folder the dashboard reads.

## Output Contract

Bronze files stay as Parquet under:

```text
bitcoin_pipeline/data/bronze/{source}/{dataset}/year=YYYY/month=MM/day=DD/
```

dbt creates a local DuckDB database at:

```text
bitcoin_pipeline/data/bitcoin_pipeline.duckdb
```

The dashboard reads only Gold marts:

- `mart_btc_price_analysis`
- `mart_btc_sentiment_correlation`
- `mart_btc_volume_anomalies`

## Data Quality Checks

Run:

```powershell
dbt test
```

Current checks cover:

- Not-null dates, symbols, close price, and sentiment fields
- Accepted Binance intervals
- Fear & Greed value range from 0 to 100
- Positive price and non-negative volume sanity checks

## Interview Talking Points

- Medallion architecture: raw Bronze Parquet, typed Silver tables, business Gold marts.
- DuckDB keeps the demo simple while still using SQL transformation patterns close to warehouse work.
- dbt models document lineage from raw API outputs to recruiter-visible metrics.
- The dashboard is intentionally downstream-only: it queries Gold marts, not raw files.
- Roadmap phases add MinIO/S3 compatibility, Airflow orchestration, Great Expectations, and cloud deployment after the MVP is working.

## Environment Variables

| Variable | Purpose | Default |
|---|---|---|
| `DATA_DIR` | Local data root for Bronze Parquet | `bitcoin_pipeline/data` |
| `DUCKDB_PATH` | DuckDB database file path | `bitcoin_pipeline/data/bitcoin_pipeline.duckdb` |
| `S3_BUCKET` | Future S3/MinIO bucket name | `bitcoin-pipeline-bronze` |
| `AWS_REGION` | AWS region for future cloud resources | `ap-southeast-1` |
| `S3_ENDPOINT_URL` | Optional MinIO/S3-compatible endpoint | empty |
| `AWS_ACCESS_KEY_ID` | Optional AWS/MinIO access key | empty |
| `AWS_SECRET_ACCESS_KEY` | Optional AWS/MinIO secret key | empty |
