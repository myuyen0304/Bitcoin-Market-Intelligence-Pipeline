"""Unit tests for ``CoinGeckoFetcher._parse_market_chart`` (no network).

The parser is the one piece of real transform logic in ingestion: it merges the
three parallel lists CoinGecko returns (``prices`` / ``market_caps`` /
``total_volumes``) into one tidy daily row, renames columns, derives ``date``
from the epoch-ms timestamp, and drops the raw timestamp. These tests feed a
hand-built response so they run offline and pin that contract.
"""

from __future__ import annotations

import pandas as pd

from ingestion.sources.coingecko import CoinGeckoFetcher

# Two days of fake CoinGecko market_chart payload. Timestamps are midnight UTC
# for 2024-01-01 and 2024-01-02 in epoch-milliseconds.
_TS_DAY1 = 1_704_067_200_000  # 2024-01-01T00:00:00Z
_TS_DAY2 = 1_704_153_600_000  # 2024-01-02T00:00:00Z

_RAW = {
    "prices": [[_TS_DAY1, 42_000.0], [_TS_DAY2, 43_500.0]],
    "market_caps": [[_TS_DAY1, 8.0e11], [_TS_DAY2, 8.2e11]],
    "total_volumes": [[_TS_DAY1, 2.0e10], [_TS_DAY2, 2.5e10]],
}


def _parse():
    # _parse_market_chart is a pure method — no HTTP session needed to call it.
    return CoinGeckoFetcher()._parse_market_chart(_RAW, coin_id="bitcoin")


def test_columns_and_order():
    df = _parse()
    assert list(df.columns) == [
        "date",
        "coin_id",
        "price_usd",
        "market_cap_usd",
        "volume_usd",
        "ingested_at",
    ]


def test_merges_three_lists_row_per_day():
    df = _parse()
    assert len(df) == 2
    # Timestamp column is dropped after deriving `date`.
    assert "timestamp_ms" not in df.columns


def test_values_land_in_the_right_columns():
    df = _parse().sort_values("date").reset_index(drop=True)
    assert df.loc[0, "price_usd"] == 42_000.0
    assert df.loc[0, "market_cap_usd"] == 8.0e11
    assert df.loc[0, "volume_usd"] == 2.0e10
    assert (df["coin_id"] == "bitcoin").all()


def test_date_is_derived_from_timestamp():
    df = _parse().sort_values("date").reset_index(drop=True)
    assert pd.api.types.is_datetime64_any_dtype(df["date"])
    assert df.loc[0, "date"] == pd.Timestamp("2024-01-01", tz="UTC")
    assert df.loc[1, "date"] == pd.Timestamp("2024-01-02", tz="UTC")


def test_misaligned_timestamps_inner_join_drops_unmatched():
    # market_caps missing day2 → inner merge keeps only the shared day.
    raw = {
        "prices": [[_TS_DAY1, 42_000.0], [_TS_DAY2, 43_500.0]],
        "market_caps": [[_TS_DAY1, 8.0e11]],
        "total_volumes": [[_TS_DAY1, 2.0e10], [_TS_DAY2, 2.5e10]],
    }
    df = CoinGeckoFetcher()._parse_market_chart(raw, coin_id="bitcoin")
    assert len(df) == 1
    assert df.loc[0, "date"] == pd.Timestamp("2024-01-01", tz="UTC")
