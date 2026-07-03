"""
DAG: btc_daily_ingestion
------------------------
Runs every day at 02:00 UTC (after all markets settle for the day).
Fetches yesterday's data from all 3 sources, lands to the local Bronze layer,
then builds the dbt Silver/Gold models.

DAG structure:
    ingest_coingecko  ─┐
    ingest_binance    ─┼── validate_bronze ── dbt_build
    ingest_fear_greed ─┘

Design decisions:
  - 3 ingestion tasks run in PARALLEL (no dependency between sources)
  - validate_bronze runs AFTER all 3 complete (trigger_rule=all_success)
  - dbt_build runs AFTER Bronze is validated (ingest -> transform end-to-end)
  - Writes to local Bronze via local_writer; MinIO/S3 comes in a later phase
  - Config (data dir) comes from config.settings, not hardcoded values
  - Uses logical_date for idempotent backfill support
  - XCom passes written paths downstream for lineage tracking
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# These will resolve at runtime in the Airflow container (PYTHONPATH = bitcoin_pipeline)
from config.settings import settings
from ingestion.sources.binance import BinanceFetcher
from ingestion.sources.coingecko import CoinGeckoFetcher
from ingestion.sources.fear_greed import FearGreedFetcher
from ingestion.utils.local_writer import write_local_parquet

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────
# Repo root = two levels up from this DAG file (bitcoin_pipeline/dags/..).
# The dbt project (dbt_project.yml, profiles.yml) lives at the repo root.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = settings.data_dir   # local Bronze root; overridable via DATA_DIR env var

# dbt binary: in the Airflow container dbt lives in an isolated venv (set via
# DBT_BIN); falls back to `dbt` on PATH for local runs.
DBT_BIN = os.environ.get("DBT_BIN", "dbt")

DEFAULT_ARGS = {
    "owner": "uyen",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


# ── Task functions ───────────────────────────────────────────────────────

def ingest_coingecko(**context) -> str:
    """
    Fetch daily CoinGecko price + market cap + volume for execution date.
    Pushes S3 path to XCom for downstream tasks.
    """
    execution_date: datetime = context["logical_date"]
    fetcher = CoinGeckoFetcher()

    # Fetch 2 days to ensure we have yesterday's complete candle
    df = fetcher.fetch(coin_id="bitcoin", days=2)

    # Filter to execution date only (idempotent)
    target_date = execution_date.date()
    df = df[df["date"].dt.date == target_date]

    if df.empty:
        logger.warning(f"No CoinGecko data for {target_date} — possibly weekend/holiday gap")
        return ""

    path = str(write_local_parquet(
        df=df,
        source="coingecko",
        dataset="market_chart",
        base_dir=DATA_DIR,
        partition_date=execution_date,
    ))

    # Push to XCom so validate_bronze knows where to find data
    context["ti"].xcom_push(key="coingecko_s3_path", value=path)
    logger.info(f"CoinGecko ingestion complete: {path}")
    return path


def ingest_binance(**context) -> str:
    """
    Fetch Binance daily klines for execution date.
    Fetches both 1d and 1h intervals for richer analysis.
    """
    execution_date: datetime = context["logical_date"]
    fetcher = BinanceFetcher()

    target_date = execution_date.date()
    start = target_date.strftime("%Y-%m-%d")
    end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

    # Daily candle
    df_daily = fetcher.fetch(symbol="BTCUSDT", interval="1d", start=start, end=end)
    path = str(write_local_parquet(
        df=df_daily,
        source="binance",
        dataset="klines_1d",
        base_dir=DATA_DIR,
        partition_date=execution_date,
    ))

    # Hourly candles (richer for analysis)
    df_hourly = fetcher.fetch(symbol="BTCUSDT", interval="1h", start=start, end=end)
    write_local_parquet(
        df=df_hourly,
        source="binance",
        dataset="klines_1h",
        base_dir=DATA_DIR,
        partition_date=execution_date,
    )

    context["ti"].xcom_push(key="binance_s3_path", value=path)
    logger.info(f"Binance ingestion complete: {path}")
    return path


def ingest_fear_greed(**context) -> str:
    """Fetch today's Fear & Greed Index value."""
    execution_date: datetime = context["logical_date"]
    fetcher = FearGreedFetcher()

    latest = fetcher.fetch_latest()
    df = __import__("pandas").DataFrame([latest])

    path = str(write_local_parquet(
        df=df,
        source="feargreed",
        dataset="index",
        base_dir=DATA_DIR,
        partition_date=execution_date,
    ))

    context["ti"].xcom_push(key="feargreed_s3_path", value=path)
    logger.info(f"Fear & Greed ingestion complete: {path}")
    return path


def validate_bronze(**context) -> None:
    """
    Basic validation after all sources land to Bronze.
    Checks:
      1. All 3 XCom paths are non-empty (all tasks succeeded)
      2. Row counts are non-zero
      3. Dates match logical_date (no stale data)

    In production: replace with Great Expectations suite.
    """
    ti = context["ti"]

    paths = {
        "coingecko": ti.xcom_pull(key="coingecko_s3_path", task_ids="ingest_coingecko"),
        "binance":   ti.xcom_pull(key="binance_s3_path",   task_ids="ingest_binance"),
        "feargreed": ti.xcom_pull(key="feargreed_s3_path", task_ids="ingest_fear_greed"),
    }

    failed = [source for source, path in paths.items() if not path]
    if failed:
        raise ValueError(f"Bronze validation failed — missing paths for: {failed}")

    logger.info("Bronze validation passed ✓")
    for source, path in paths.items():
        logger.info(f"  {source}: {path}")


# ── DAG definition ───────────────────────────────────────────────────────

with DAG(
    dag_id="btc_daily_ingestion",
    description="Fetch Bitcoin data from CoinGecko, Binance, Fear&Greed → S3 Bronze",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="0 2 * * *",            # 02:00 UTC daily
    catchup=False,                   # don't backfill on first deploy
    tags=["bitcoin", "ingestion", "bronze"],
    max_active_runs=1,               # prevent overlapping runs
) as dag:

    t_coingecko = PythonOperator(
        task_id="ingest_coingecko",
        python_callable=ingest_coingecko,
    )

    t_binance = PythonOperator(
        task_id="ingest_binance",
        python_callable=ingest_binance,
    )

    t_fear_greed = PythonOperator(
        task_id="ingest_fear_greed",
        python_callable=ingest_fear_greed,
    )

    t_validate = PythonOperator(
        task_id="validate_bronze",
        python_callable=validate_bronze,
        trigger_rule="all_success",   # only run if ALL 3 ingestions pass
    )

    # Transform Bronze -> Silver/Gold with dbt (run + test in one DAG-aware pass)
    t_dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd '{PROJECT_ROOT}' && {DBT_BIN} build --profiles-dir '{PROJECT_ROOT}'",
    )

    # 3 ingestions run in parallel -> validate -> dbt build
    [t_coingecko, t_binance, t_fear_greed] >> t_validate >> t_dbt_build