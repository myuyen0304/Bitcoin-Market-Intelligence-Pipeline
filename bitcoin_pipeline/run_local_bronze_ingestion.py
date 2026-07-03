"""
Run local Bronze ingestion using real public APIs.

Fetches Bitcoin market data from CoinGecko, Binance, and Fear & Greed Index,
validates each DataFrame against the Bronze schema, writes Hive-partitioned
Parquet files to bitcoin_pipeline/data/bronze/, then prints a run summary.

Writer note: write_local_parquet() is intentionally local-only. The S3-compatible
equivalent (S3Writer) is used in Phase 2+ when MinIO/AWS is available.
"""

import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from ingestion.sources.binance import BinanceFetcher
from ingestion.sources.coingecko import CoinGeckoFetcher
from ingestion.sources.fear_greed import FearGreedFetcher
from ingestion.utils.local_writer import write_local_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-dataset schema specification
# ---------------------------------------------------------------------------

class DatasetSpec(NamedTuple):
    source: str
    dataset: str
    required_cols: list[str]
    non_null_cols: list[str]
    time_col: str   # column used for min/max timestamp in summary


DATASET_SPECS: list[DatasetSpec] = [
    DatasetSpec(
        source="coingecko",
        dataset="market_chart",
        required_cols=["date", "coin_id", "price_usd", "market_cap_usd", "volume_usd", "ingested_at"],
        non_null_cols=["date", "coin_id", "price_usd", "ingested_at"],
        time_col="date",
    ),
    DatasetSpec(
        source="binance",
        dataset="klines_1d",
        # Binance returns 2 extra cols (taker_buy_*) not in target.md — use subset check
        required_cols=["open_time", "close_time", "symbol", "interval",
                       "open", "high", "low", "close", "volume", "quote_volume",
                       "num_trades", "ingested_at"],
        non_null_cols=["open_time", "close_time", "symbol", "open", "close", "ingested_at"],
        time_col="open_time",
    ),
    DatasetSpec(
        source="binance",
        dataset="klines_1h",
        required_cols=["open_time", "close_time", "symbol", "interval",
                       "open", "high", "low", "close", "volume", "quote_volume",
                       "num_trades", "ingested_at"],
        non_null_cols=["open_time", "close_time", "symbol", "open", "close", "ingested_at"],
        time_col="open_time",
    ),
    DatasetSpec(
        source="feargreed",
        dataset="index",
        required_cols=["date", "fear_greed_value", "sentiment_label", "ingested_at"],
        non_null_cols=["date", "fear_greed_value", "sentiment_label", "ingested_at"],
        time_col="date",
    ),
]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_dataframe(df: pd.DataFrame, spec: DatasetSpec) -> list[str]:
    """Validate a fetched DataFrame against its schema spec.

    Returns a list of error strings; empty means the DataFrame passed.
    """
    errors: list[str] = []

    if df.empty:
        errors.append("DataFrame is empty")
        return errors  # no point checking further

    missing = [c for c in spec.required_cols if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")

    null_errors = [
        f"Column '{c}' has {df[c].isnull().sum()} null(s)"
        for c in spec.non_null_cols
        if c in df.columns and df[c].isnull().any()
    ]
    errors.extend(null_errors)

    if spec.time_col in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df[spec.time_col]):
            errors.append(f"Column '{spec.time_col}' is not a datetime type (got {df[spec.time_col].dtype})")

    return errors


# ---------------------------------------------------------------------------
# Summary record
# ---------------------------------------------------------------------------

@dataclass
class FetchResult:
    source: str
    dataset: str
    status: str = "PENDING"
    row_count: int = 0
    min_ts: str = "—"
    max_ts: str = "—"
    output_path: str = "—"
    errors: list[str] = field(default_factory=list)


def _extract_ts(df: pd.DataFrame, time_col: str) -> tuple[str, str]:
    """Return (min, max) timestamps as ISO strings from a time column."""
    try:
        col = df[time_col]
        return str(col.min())[:19], str(col.max())[:19]
    except Exception:
        return "—", "—"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Fetch real API data, validate, write local Bronze Parquet, print summary."""
    run_date = datetime.now(timezone.utc)
    data_dir = PROJECT_DIR / "data"

    logger.info("=" * 60)
    logger.info("Bronze local ingestion started at %s", run_date.strftime("%Y-%m-%dT%H:%M:%SZ"))
    logger.info("=" * 60)

    results: list[FetchResult] = []

    # --- CoinGecko -----------------------------------------------------------
    r_cg = FetchResult(source="coingecko", dataset="market_chart")
    spec_cg = next(s for s in DATASET_SPECS if s.dataset == "market_chart")
    try:
        logger.info("[coingecko] Fetching market_chart ...")
        df = CoinGeckoFetcher().fetch(coin_id="bitcoin", days=30)
        errs = validate_dataframe(df, spec_cg)
        if errs:
            r_cg.status = "FAIL"
            r_cg.errors = errs
            logger.error("[coingecko] Validation failed: %s", errs)
        else:
            path = write_local_parquet(df, "coingecko", "market_chart", data_dir, run_date)
            r_cg.status = "PASS"
            r_cg.row_count = len(df)
            r_cg.min_ts, r_cg.max_ts = _extract_ts(df, spec_cg.time_col)
            r_cg.output_path = str(path)
            logger.info("[coingecko] PASS - %d rows", len(df))
    except Exception as exc:
        r_cg.status = "ERROR"
        r_cg.errors = [str(exc)]
        logger.exception("[coingecko] Fetch failed: %s", exc)
    results.append(r_cg)

    # --- Binance 1d ----------------------------------------------------------
    r_b1d = FetchResult(source="binance", dataset="klines_1d")
    spec_b = next(s for s in DATASET_SPECS if s.dataset == "klines_1d")
    try:
        logger.info("[binance] Fetching klines_1d ...")
        df = BinanceFetcher().fetch(symbol="BTCUSDT", interval="1d", start="2024-01-01")
        errs = validate_dataframe(df, spec_b)
        if errs:
            r_b1d.status = "FAIL"
            r_b1d.errors = errs
            logger.error("[binance] klines_1d validation failed: %s", errs)
        else:
            path = write_local_parquet(df, "binance", "klines_1d", data_dir, run_date)
            r_b1d.status = "PASS"
            r_b1d.row_count = len(df)
            r_b1d.min_ts, r_b1d.max_ts = _extract_ts(df, spec_b.time_col)
            r_b1d.output_path = str(path)
            logger.info("[binance] klines_1d PASS - %d rows", len(df))
    except Exception as exc:
        r_b1d.status = "ERROR"
        r_b1d.errors = [str(exc)]
        logger.exception("[binance] klines_1d fetch failed: %s", exc)
    results.append(r_b1d)

    # --- Binance 1h ----------------------------------------------------------
    r_b1h = FetchResult(source="binance", dataset="klines_1h")
    spec_b1h = next(s for s in DATASET_SPECS if s.dataset == "klines_1h")
    try:
        logger.info("[binance] Fetching klines_1h ...")
        df = BinanceFetcher().fetch(symbol="BTCUSDT", interval="1h", start="2026-05-01")
        errs = validate_dataframe(df, spec_b1h)
        if errs:
            r_b1h.status = "FAIL"
            r_b1h.errors = errs
            logger.error("[binance] klines_1h validation failed: %s", errs)
        else:
            path = write_local_parquet(df, "binance", "klines_1h", data_dir, run_date)
            r_b1h.status = "PASS"
            r_b1h.row_count = len(df)
            r_b1h.min_ts, r_b1h.max_ts = _extract_ts(df, spec_b1h.time_col)
            r_b1h.output_path = str(path)
            logger.info("[binance] klines_1h PASS - %d rows", len(df))
    except Exception as exc:
        r_b1h.status = "ERROR"
        r_b1h.errors = [str(exc)]
        logger.exception("[binance] klines_1h fetch failed: %s", exc)
    results.append(r_b1h)

    # --- Fear & Greed --------------------------------------------------------
    r_fg = FetchResult(source="feargreed", dataset="index")
    spec_fg = next(s for s in DATASET_SPECS if s.dataset == "index")
    try:
        logger.info("[feargreed] Fetching index ...")
        df = FearGreedFetcher().fetch(limit=365)
        errs = validate_dataframe(df, spec_fg)
        if errs:
            r_fg.status = "FAIL"
            r_fg.errors = errs
            logger.error("[feargreed] Validation failed: %s", errs)
        else:
            path = write_local_parquet(df, "feargreed", "index", data_dir, run_date)
            r_fg.status = "PASS"
            r_fg.row_count = len(df)
            r_fg.min_ts, r_fg.max_ts = _extract_ts(df, spec_fg.time_col)
            r_fg.output_path = str(path)
            logger.info("[feargreed] PASS - %d rows", len(df))
    except Exception as exc:
        r_fg.status = "ERROR"
        r_fg.errors = [str(exc)]
        logger.exception("[feargreed] Fetch failed: %s", exc)
    results.append(r_fg)

    # --- Run summary ---------------------------------------------------------
    print("\n" + "=" * 80)
    print(f"  BRONZE INGESTION SUMMARY  |  {run_date.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print("=" * 80)
    print(f"  {'SOURCE':<12} {'DATASET':<14} {'STATUS':<7} {'ROWS':>6}  {'MIN TIMESTAMP':<20} {'MAX TIMESTAMP':<20}")
    print("  " + "-" * 76)

    any_failed = False
    for r in results:
        status_icon = "OK" if r.status == "PASS" else "!!"
        print(
            f"  {r.source:<12} {r.dataset:<14} [{status_icon}] {r.status:<5} "
            f"{r.row_count:>6}  {r.min_ts:<20} {r.max_ts:<20}"
        )
        if r.errors:
            for e in r.errors:
                print(f"              └─ {e}")
        if r.status != "PASS":
            any_failed = True

    print("=" * 80)

    passed = sum(1 for r in results if r.status == "PASS")
    print(f"  {passed}/{len(results)} datasets passed\n")

    if any_failed:
        logger.error("One or more datasets failed — review errors above.")
        sys.exit(1)

    logger.info("Bronze local ingestion complete - all datasets passed.")


if __name__ == "__main__":
    main()
