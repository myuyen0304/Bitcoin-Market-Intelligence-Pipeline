"""Unit tests for the Great Expectations Bronze gate (offline).

These prove the gate does its job: clean Bronze passes, and each source's most
important invariant, when violated, makes ``run_bronze_checks`` return
``success=False`` — the signal the DAG uses to stop before dbt runs. GE runs
purely on an in-memory pandas frame here, so there is no network and no MinIO;
the only I/O is a Parquet round-trip through a tmp file, mirroring how the real
task reads Bronze back before validating.
"""

from __future__ import annotations

import pandas as pd
import pytest

from quality import bronze_checkpoint

# --- Clean fixtures: exactly the columns each suite asserts on. ---------------


def _clean_coingecko() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True),
            "coin_id": ["bitcoin", "bitcoin"],
            "price_usd": [42_000.0, 43_500.0],
            "market_cap_usd": [8.0e11, 8.2e11],
            "volume_usd": [2.0e10, 2.5e10],
            "ingested_at": pd.Timestamp.now(tz="UTC"),
        }
    )


def _clean_binance() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "interval": ["1d", "1d"],
            "open": [42_000.0, 43_000.0],
            "high": [43_000.0, 44_000.0],
            "low": [41_500.0, 42_800.0],
            "close": [42_800.0, 43_900.0],
            "volume": [1200.0, 1500.0],
        }
    )


def _clean_feargreed() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True),
            "fear_greed_value": [55, 72],
            "sentiment_label": ["Greed", "Greed"],
            "sentiment_bucket": ["Greed", "Greed"],
        }
    )


def _run(monkeypatch, tmp_path, source: str, df: pd.DataFrame) -> bool:
    """Write ``df`` to Parquet and run the real Bronze gate against it."""
    # Keep GE's HTML Data Docs out of the repo tree during tests.
    monkeypatch.setattr(bronze_checkpoint, "GX_DOCS_ROOT", tmp_path / "gx_docs")
    parquet = tmp_path / f"{source}.parquet"
    df.to_parquet(parquet, index=False, engine="pyarrow")
    return bronze_checkpoint.run_bronze_checks(source, str(parquet))["success"]


# --- Clean data must pass ------------------------------------------------------


@pytest.mark.parametrize(
    "source,builder",
    [
        ("coingecko", _clean_coingecko),
        ("binance", _clean_binance),
        ("feargreed", _clean_feargreed),
    ],
)
def test_clean_bronze_passes(monkeypatch, tmp_path, source, builder):
    assert _run(monkeypatch, tmp_path, source, builder()) is True


# --- Each source's key invariant, violated, must fail --------------------------


def test_coingecko_negative_price_fails(monkeypatch, tmp_path):
    df = _clean_coingecko()
    df.loc[0, "price_usd"] = -1.0  # a zero/negative price is a bad candle
    assert _run(monkeypatch, tmp_path, "coingecko", df) is False


def test_binance_high_below_low_fails(monkeypatch, tmp_path):
    df = _clean_binance()
    df.loc[0, "high"] = 40_000.0  # high now < low → impossible candle
    df.loc[0, "low"] = 41_500.0
    assert _run(monkeypatch, tmp_path, "binance", df) is False


def test_feargreed_out_of_range_fails(monkeypatch, tmp_path):
    df = _clean_feargreed()
    df.loc[0, "fear_greed_value"] = 150  # index is defined on 0–100
    assert _run(monkeypatch, tmp_path, "feargreed", df) is False


def test_feargreed_unknown_bucket_fails(monkeypatch, tmp_path):
    df = _clean_feargreed()
    df.loc[0, "sentiment_bucket"] = "Euphoria"  # not one of our 5 buckets
    assert _run(monkeypatch, tmp_path, "feargreed", df) is False


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
