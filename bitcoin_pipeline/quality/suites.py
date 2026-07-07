"""Expectation suites for each Bronze source, defined as code.

Suites are the source of truth for Bronze data quality. Defining them in Python
(rather than a GUI or a JSON blob) keeps them diffable and reviewable like any
other code, and lets the checkpoint rebuild them deterministically on every run.

Each expectation mirrors an invariant the raw API data must hold *before* dbt
runs. Where a check overlaps a downstream dbt test (e.g. ``fear_greed_value``
range, ``sentiment_bucket`` accepted values), GE enforces it earlier — at the
Bronze input — so bad data never reaches the transform layer at all.

Column names are verified against the actual fetcher output:
  - coingecko/market_chart: date, coin_id, price_usd, market_cap_usd, volume_usd, ingested_at
  - binance/klines_1d:      open_time, close_time, symbol, interval, open, high,
                            low, close, volume, quote_volume, num_trades, ...
  - feargreed/index:        date, fear_greed_value, sentiment_label, sentiment_bucket, ingested_at
"""

from __future__ import annotations

import great_expectations as gx
from great_expectations import expectations as gxe

# The five buckets our own classifier emits (see fear_greed.classify_sentiment).
SENTIMENT_BUCKETS = ["Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"]


def _coingecko_suite() -> gx.ExpectationSuite:
    """Bronze expectations for CoinGecko market_chart (daily price/cap/volume)."""
    suite = gx.ExpectationSuite(name="coingecko_bronze")
    for exp in [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="date"),
        gxe.ExpectColumnValuesToNotBeNull(column="coin_id"),
        gxe.ExpectColumnValuesToBeInSet(column="coin_id", value_set=["bitcoin"]),
        gxe.ExpectColumnValuesToNotBeNull(column="price_usd"),
        # Price must be strictly positive — a zero/negative price is a bad candle.
        gxe.ExpectColumnValuesToBeBetween(column="price_usd", min_value=0, strict_min=True),
        gxe.ExpectColumnValuesToNotBeNull(column="volume_usd"),
        gxe.ExpectColumnValuesToBeBetween(column="volume_usd", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="market_cap_usd", min_value=0),
    ]:
        suite.add_expectation(exp)
    return suite


def _binance_suite() -> gx.ExpectationSuite:
    """Bronze expectations for Binance klines_1d (daily OHLCV candles)."""
    suite = gx.ExpectationSuite(name="binance_bronze")
    for exp in [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToBeInSet(column="symbol", value_set=["BTCUSDT"]),
        gxe.ExpectColumnValuesToBeInSet(column="interval", value_set=["1d"]),
        # OHLC must all be strictly positive.
        gxe.ExpectColumnValuesToNotBeNull(column="open"),
        gxe.ExpectColumnValuesToBeBetween(column="open", min_value=0, strict_min=True),
        gxe.ExpectColumnValuesToBeBetween(column="high", min_value=0, strict_min=True),
        gxe.ExpectColumnValuesToBeBetween(column="low", min_value=0, strict_min=True),
        gxe.ExpectColumnValuesToNotBeNull(column="close"),
        gxe.ExpectColumnValuesToBeBetween(column="close", min_value=0, strict_min=True),
        # A candle's high can never be below its low.
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="high", column_B="low", or_equal=True
        ),
        gxe.ExpectColumnValuesToNotBeNull(column="volume"),
        gxe.ExpectColumnValuesToBeBetween(column="volume", min_value=0),
    ]:
        suite.add_expectation(exp)
    return suite


def _feargreed_suite() -> gx.ExpectationSuite:
    """Bronze expectations for the Fear & Greed index (one row per run)."""
    suite = gx.ExpectationSuite(name="feargreed_bronze")
    for exp in [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="date"),
        gxe.ExpectColumnValuesToNotBeNull(column="fear_greed_value"),
        # The most valuable check: the index is defined on 0–100.
        gxe.ExpectColumnValuesToBeBetween(column="fear_greed_value", min_value=0, max_value=100),
        # sentiment_bucket is derived by our own code → we fully control its domain.
        gxe.ExpectColumnValuesToBeInSet(column="sentiment_bucket", value_set=SENTIMENT_BUCKETS),
        # sentiment_label comes straight from the API → only assert presence.
        gxe.ExpectColumnValuesToNotBeNull(column="sentiment_label"),
    ]:
        suite.add_expectation(exp)
    return suite


# Map each Bronze source to its suite builder. Builders (not cached instances)
# because a suite gets bound to a DataContext when registered, so each run
# rebuilds a fresh one.
SUITE_BUILDERS = {
    "coingecko": _coingecko_suite,
    "binance": _binance_suite,
    "feargreed": _feargreed_suite,
}


def build_suite(source: str) -> gx.ExpectationSuite:
    """Return a fresh ExpectationSuite for a Bronze source.

    Args:
        source: One of ``coingecko``, ``binance``, ``feargreed``.

    Returns:
        A new ExpectationSuite for that source.

    Raises:
        KeyError: If ``source`` has no defined suite.
    """
    if source not in SUITE_BUILDERS:
        raise KeyError(f"No expectation suite defined for source '{source}'. "
                       f"Known: {sorted(SUITE_BUILDERS)}")
    return SUITE_BUILDERS[source]()
