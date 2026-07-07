"""
DAG: btc_daily_ingestion
------------------------
Runs every day at 08:30 Vietnam time (Asia/Ho_Chi_Minh).
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
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode, InvocationMode, LoadMode

# These will resolve at runtime in the Airflow container (PYTHONPATH = bitcoin_pipeline)
from ingestion.sources.binance import BinanceFetcher
from ingestion.sources.coingecko import CoinGeckoFetcher
from ingestion.sources.fear_greed import FearGreedFetcher
from ingestion.utils.bronze_writer import write_bronze

logger = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────────
# Repo root = two levels up from this DAG file (bitcoin_pipeline/dags/..).
# The dbt project (dbt_project.yml, profiles.yml) lives at the repo root.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
# Bronze destination (local disk vs MinIO/S3) is decided inside write_bronze by
# settings.s3_endpoint_url — the DAG stays agnostic to where Bronze lands.

# dbt binary: in the Airflow container dbt lives in an isolated venv (set via
# DBT_BIN); falls back to `dbt` on PATH for local runs.
DBT_BIN = os.environ.get("DBT_BIN", "dbt")

# ── Cosmos (dbt → Airflow tasks) ─────────────────────────────────────────
# Cosmos renders each dbt model/test into its own Airflow task so the dbt DAG
# shows up inside the Airflow Graph (per-model retry + lineage), instead of one
# opaque `dbt build` shell command.
#
# Key choices for THIS project:
#   - ExecutionMode.LOCAL + InvocationMode.SUBPROCESS + dbt_executable_path=DBT_BIN
#     → Cosmos runs in Airflow's env but shells out to the isolated /opt/dbt-venv,
#       so dbt's deps never leak into Airflow's env.
#   - profiles_yml_filepath reuses the repo's profiles.yml (dbt-duckdb) — no need
#     for an Airflow connection.
#   - LoadMode.DBT_MANIFEST → Cosmos builds the task graph by reading the dbt
#     manifest (target/manifest.json) at parse time. We do NOT use LoadMode.DBT_LS
#     here because `dbt_project_path` is the whole repo root: Cosmos would copy
#     and hash the entire repo (.git, .venv, data/, target/) on every parse and
#     blow past the DAG import timeout. The manifest is regenerated on every run
#     by the dbt tasks themselves (and by any local `dbt build`/`dbt parse`).
DBT_PROFILES_YML = PROJECT_ROOT / "profiles.yml"
DBT_MANIFEST = PROJECT_ROOT / "target" / "manifest.json"

dbt_project_config = ProjectConfig(
    dbt_project_path=PROJECT_ROOT,   # dbt_project.yml lives at the repo root
    project_name="bitcoin_pipeline",
    # Our dbt_project.yml sets a custom model-paths; Cosmos otherwise looks for
    # a top-level models/ dir and fails validation. Point it at the real path.
    models_relative_path="bitcoin_pipeline/dbt/models",
    # Build the Airflow task graph from this manifest instead of running dbt ls.
    manifest_path=DBT_MANIFEST,
    # No packages.yml in this project → skip `dbt deps` (one less subprocess).
    install_dbt_deps=False,
)
dbt_profile_config = ProfileConfig(
    profile_name="bitcoin_pipeline",
    # `dev` reads Bronze from local disk; `minio` reads it from MinIO/S3 via
    # httpfs. The Airflow stack sets DBT_TARGET=minio; local runs default to dev.
    target_name=os.environ.get("DBT_TARGET", "dev"),
    profiles_yml_filepath=DBT_PROFILES_YML,
)
dbt_execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path=DBT_BIN,
)
dbt_render_config = RenderConfig(
    load_method=LoadMode.DBT_MANIFEST,
    # dbt is not importable in Airflow's own env; SUBPROCESS avoids the DBT_RUNNER
    # code path (which would require dbt installed alongside Airflow).
    invocation_mode=InvocationMode.SUBPROCESS,
    dbt_executable_path=DBT_BIN,
)

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

    path = write_bronze(
        df=df,
        source="coingecko",
        dataset="market_chart",
        partition_date=execution_date,
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
    execution_date: datetime = context["logical_date"]
    fetcher = BinanceFetcher()

    target_date = execution_date.date()
    start = target_date.strftime("%Y-%m-%d")
    end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

    # Daily candle
    df_daily = fetcher.fetch(symbol="BTCUSDT", interval="1d", start=start, end=end)
    path = write_bronze(
        df=df_daily,
        source="binance",
        dataset="klines_1d",
        partition_date=execution_date,
    )

    # Hourly candles (richer for analysis)
    df_hourly = fetcher.fetch(symbol="BTCUSDT", interval="1h", start=start, end=end)
    write_bronze(
        df=df_hourly,
        source="binance",
        dataset="klines_1h",
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

    path = write_bronze(
        df=df,
        source="feargreed",
        dataset="index",
        partition_date=execution_date,
    )

    context["ti"].xcom_push(key="feargreed_s3_path", value=path)
    logger.info(f"Fear & Greed ingestion complete: {path}")
    return path


def validate_bronze(**context) -> None:
    """
    Quality gate between ingestion and transform.

    Two steps, cheap-check first:
      1. All 3 XCom paths are non-empty (every ingest task produced a file).
      2. Each Bronze Parquet passes its Great Expectations suite (row count,
         not-null keys, value ranges — e.g. price > 0, fear_greed in 0–100,
         high >= low). GE reads the file back storage-agnostically (local or
         s3://), so this gates the same data dbt is about to read.

    Raising here (all_success downstream) stops dbt_build from running on bad
    Bronze — GE validates the *input*, dbt tests validate the *output*.
    """
    ti = context["ti"]

    paths = {
        "coingecko": ti.xcom_pull(key="coingecko_s3_path", task_ids="ingest_coingecko"),
        "binance":   ti.xcom_pull(key="binance_s3_path",   task_ids="ingest_binance"),
        "feargreed": ti.xcom_pull(key="feargreed_s3_path", task_ids="ingest_fear_greed"),
    }

    missing = [source for source, path in paths.items() if not path]
    if missing:
        raise AirflowException(f"Bronze validation failed — missing paths for: {missing}")

    # Import GE lazily inside the task: it is a heavy import and would slow DAG
    # parsing if pulled in at module top.
    from quality.bronze_checkpoint import run_bronze_checks

    failed = [
        source for source, path in paths.items()
        if not run_bronze_checks(source, path)["success"]
    ]
    if failed:
        raise AirflowException(f"Great Expectations Bronze validation FAILED for: {failed}")

    logger.info("GE Bronze validation passed for all sources ✓")


# ── DAG definition ───────────────────────────────────────────────────────

with DAG(
    dag_id="btc_daily_ingestion",
    description="Fetch Bitcoin data from CoinGecko, Binance, Fear&Greed → S3 Bronze",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule="30 8 * * *",           # 08:30 giờ VN (Asia/Ho_Chi_Minh) mỗi ngày
    catchup=False,                   # don't backfill on first deploy
    is_paused_upon_creation=False,   # start unpaused (auto-run without manual toggle)
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
        # Data-quality gate: a real GE failure means bad data, which a retry won't
        # fix — so fail fast instead of the DAG's 2×5-min exponential backoff. One
        # short retry still absorbs a transient MinIO/network blip on the read-back.
        retries=1,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=False,
    )

    # Transform Bronze -> Silver/Gold with dbt, rendered by Cosmos into one
    # Airflow task per model/test (staging -> intermediate -> marts).
    #
    # DuckDB is a single-writer database: only ONE process may open the file for
    # write at a time. The 3 staging models have no dependency between them, so
    # Cosmos would otherwise run them in parallel -> DuckDB lock conflict. We pin
    # every dbt task to the `duckdb_serial` pool (1 slot) to serialize them.
    # Reading Bronze parquet from object storage (MinIO/S3) over httpfs can hit an
    # occasional cold-cache miss (e.g. a transient "column not found" while the
    # parquet footer is fetched). These clear on a quick re-read, so give dbt tasks
    # a short, fixed retry instead of inheriting the DAG's 5-min exponential backoff
    # (which would stall the whole run for many minutes on a blip).
    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=dbt_project_config,
        profile_config=dbt_profile_config,
        execution_config=dbt_execution_config,
        render_config=dbt_render_config,
        operator_args={
            "pool": "duckdb_serial",
            "retries": 3,
            "retry_delay": timedelta(seconds=20),
            "retry_exponential_backoff": False,
        },
    )

    # 3 ingestions run in parallel -> validate -> dbt (per-model task group)
    [t_coingecko, t_binance, t_fear_greed] >> t_validate >> dbt_build