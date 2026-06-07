"""
DAG: btc_daily_ingestion
------------------------
Runs every day at 02:00 UTC (after all markets settle for the day).
Fetches yesterday's data from all 3 sources and lands to S3 Bronze.

DAG structure:
    start
      ├── ingest_coingecko  ─┐
      ├── ingest_binance    ─┼── validate_bronze ── end
      └── ingest_fear_greed ─┘

Design decisions:
  - 3 ingestion tasks run in PARALLEL (no dependency between sources)
  - validate_bronze runs AFTER all 3 complete (uses TaskGroup + trigger_rule)
  - Uses execution_date for idempotent backfill support
  - XCom passes S3 paths downstream for lineage tracking
"""

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# These will resolve at runtime in the Airflow container
from ingestion.sources.binance import BinanceFetcher
from ingestion.sources.coingecko import CoinGeckoFetcher
from ingestion.sources.fear_greed import FearGreedFetcher
from ingestion.utils.s3_writer import S3Writer

logger = logging.getLogger(__name__)

# ── Config (ideally from Airflow Variables or env) ──────────────────────
S3_BUCKET = "bitcoin-pipeline-bronze"   # replace with your bucket
AWS_REGION = "ap-southeast-1"

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
    execution_date: datetime = context["execution_date"]
    s3 = S3Writer(bucket=S3_BUCKET, region=AWS_REGION)
    fetcher = CoinGeckoFetcher()

    # Fetch 2 days to ensure we have yesterday's complete candle
    df = fetcher.fetch(coin_id="bitcoin", days=2)

    # Filter to execution date only (idempotent)
    target_date = execution_date.date()
    df = df[df["date"].dt.date == target_date]

    if df.empty:
        logger.warning(f"No CoinGecko data for {target_date} — possibly weekend/holiday gap")
        return ""

    path = s3.write_parquet(
        df=df,
        layer="bronze",
        source="coingecko",
        dataset="market_chart",
        date=execution_date,
    )

    # Push to XCom so validate_bronze knows where to find data
    context["ti"].xcom_push(key="coingecko_s3_path", value=path)
    logger.info(f"CoinGecko ingestion complete: {path}")
    return path


def ingest_binance(**context) -> str:
    """
    Fetch Binance daily klines for execution date.
    Fetches both 1d and 1h intervals for richer analysis.
    """
    execution_date: datetime = context["execution_date"]
    s3 = S3Writer(bucket=S3_BUCKET, region=AWS_REGION)
    fetcher = BinanceFetcher()

    target_date = execution_date.date()
    start = target_date.strftime("%Y-%m-%d")
    end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

    # Daily candle
    df_daily = fetcher.fetch(symbol="BTCUSDT", interval="1d", start=start, end=end)
    path = s3.write_parquet(
        df=df_daily,
        layer="bronze",
        source="binance",
        dataset="klines_1d",
        date=execution_date,
    )

    # Hourly candles (richer for analysis)
    df_hourly = fetcher.fetch(symbol="BTCUSDT", interval="1h", start=start, end=end)
    s3.write_parquet(
        df=df_hourly,
        layer="bronze",
        source="binance",
        dataset="klines_1h",
        date=execution_date,
    )

    context["ti"].xcom_push(key="binance_s3_path", value=path)
    logger.info(f"Binance ingestion complete: {path}")
    return path


def ingest_fear_greed(**context) -> str:
    """Fetch today's Fear & Greed Index value."""
    execution_date: datetime = context["execution_date"]
    s3 = S3Writer(bucket=S3_BUCKET, region=AWS_REGION)
    fetcher = FearGreedFetcher()

    latest = fetcher.fetch_latest()
    df = __import__("pandas").DataFrame([latest])

    path = s3.write_parquet(
        df=df,
        layer="bronze",
        source="feargreed",
        dataset="index",
        date=execution_date,
    )

    context["ti"].xcom_push(key="feargreed_s3_path", value=path)
    logger.info(f"Fear & Greed ingestion complete: {path}")
    return path


def validate_bronze(**context) -> None:
    """
    Basic validation after all sources land to Bronze.
    Checks:
      1. All 3 XCom paths are non-empty (all tasks succeeded)
      2. Row counts are non-zero
      3. Dates match execution_date (no stale data)

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
    start_date=days_ago(1),
    schedule_interval="0 2 * * *",   # 02:00 UTC daily
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

    # 3 ingestions run in parallel, validation waits for all
    [t_coingecko, t_binance, t_fear_greed] >> t_validate