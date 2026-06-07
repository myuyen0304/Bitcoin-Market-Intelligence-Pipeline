"""
Fear & Greed Index Fetcher
--------------------------
Fetches the Crypto Fear & Greed Index from alternative.me.
Free, no API key, no rate limit issues.

Index value: 0–100
  0–24   = Extreme Fear
  25–49  = Fear
  50     = Neutral
  51–74  = Greed
  75–100 = Extreme Greed

Why this matters for the project:
  - Leading indicator for price sentiment
  - Interesting correlation analysis with BTC price movement
  - Easy join key with CoinGecko/Binance data (daily granularity)

Usage:
    fetcher = FearGreedFetcher()
    df = fetcher.fetch(limit=365)
"""

import logging
from datetime import datetime, timezone

import pandas as pd

from ingestion.utils.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

# Classification buckets — used to add a human-readable label column
SENTIMENT_LABELS = {
    (0, 25): "Extreme Fear",
    (25, 50): "Fear",
    (50, 51): "Neutral",
    (51, 75): "Greed",
    (75, 101): "Extreme Greed",
}


def classify_sentiment(value: int) -> str:
    for (low, high), label in SENTIMENT_LABELS.items():
        if low <= value < high:
            return label
    return "Unknown"


class FearGreedFetcher(BaseFetcher):
    BASE_URL = "https://api.alternative.me"

    def fetch(self, limit: int = 365) -> pd.DataFrame:
        """
        Fetch Fear & Greed Index history.

        Args:
            limit: Number of days to fetch (max ~2000, daily data since 2018)

        Returns:
            DataFrame with columns: date, value, sentiment_label, ingested_at
        """
        logger.info(f"Fetching Fear & Greed Index | limit={limit} days")

        raw = self.get("/fng/", params={"limit": limit, "format": "json"})

        df = self._parse_response(raw)
        logger.info(f"Fear & Greed: {len(df)} days fetched")
        return df

    def fetch_latest(self) -> dict:
        """
        Fetch only today's value — used in incremental Airflow runs.
        Returns a single dict (not a DataFrame).
        """
        raw = self.get("/fng/", params={"limit": 1, "format": "json"})
        entry = raw["data"][0]
        return {
            "date": datetime.fromtimestamp(int(entry["timestamp"]), tz=timezone.utc).date(),
            "value": int(entry["value"]),
            "sentiment_label": entry["value_classification"],
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------ #
    #  Private helpers
    # ------------------------------------------------------------------ #

    def _parse_response(self, raw: dict) -> pd.DataFrame:
        """
        API response format:
        {
          "data": [
            {"value": "72", "value_classification": "Greed", "timestamp": "1704931200", ...},
            ...
          ]
        }
        """
        records = []
        for entry in raw["data"]:
            value = int(entry["value"])
            records.append({
                "date": datetime.fromtimestamp(
                    int(entry["timestamp"]), tz=timezone.utc
                ).date(),
                "fear_greed_value": value,
                # Use API's own classification (more granular than ours)
                "sentiment_label": entry["value_classification"],
                # Add our numeric bucket for easy filtering in dbt
                "sentiment_bucket": classify_sentiment(value),
                "ingested_at": datetime.now(timezone.utc),
            })

        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        # Sort ascending (oldest first) — consistent with other sources
        df = df.sort_values("date").reset_index(drop=True)
        return df