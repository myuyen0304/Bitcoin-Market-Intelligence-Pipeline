"""
CoinGecko Fetcher
-----------------
Fetches historical OHLCV + market cap data from CoinGecko's free public API.

Free tier limits:
  - 30 requests/minute
  - No API key required for basic endpoints
  - Historical data available from 2013

Usage:
    fetcher = CoinGeckoFetcher()
    df = fetcher.fetch(coin_id="bitcoin", days=365)
    df_ohlcv = fetcher.fetch_ohlcv(coin_id="bitcoin", days=90)
"""

import logging
from datetime import datetime, timezone

import pandas as pd

from ingestion.utils.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)


class CoinGeckoFetcher(BaseFetcher):
    BASE_URL = "https://api.coingecko.com/api/v3"

    # Free tier: 30 req/min → safe at 1 call per 2s
    RATE_LIMIT_SLEEP = 2.0

    def fetch(self, coin_id: str = "bitcoin", days: int = 365) -> pd.DataFrame:
        """
        Main fetch method called by Airflow tasks.
        Returns combined DataFrame with price, market_cap, volume.

        Args:
            coin_id: CoinGecko coin identifier (e.g. "bitcoin", "ethereum")
            days:    Number of days of history (max 365 on free tier for daily)
        """
        logger.info(f"Fetching CoinGecko market chart | coin={coin_id}, days={days}")

        raw = self.get(
            f"/coins/{coin_id}/market_chart",
            params={
                "vs_currency": "usd",
                "days": days,
                "interval": "daily",   # hourly available for <=90 days
            },
        )

        df = self._parse_market_chart(raw, coin_id)
        logger.info(f"CoinGecko: {len(df)} rows fetched for {coin_id}")
        return df

    def fetch_ohlcv(self, coin_id: str = "bitcoin", days: int = 90) -> pd.DataFrame:
        """
        Fetch OHLCV candlestick data.
        CoinGecko OHLC endpoint returns 4h candles for <= 90 days.
        """
        logger.info(f"Fetching CoinGecko OHLCV | coin={coin_id}, days={days}")
        self.rate_limit_sleep(self.RATE_LIMIT_SLEEP)

        raw = self.get(
            f"/coins/{coin_id}/ohlc",
            params={"vs_currency": "usd", "days": days},
        )

        # Raw format: [[timestamp_ms, open, high, low, close], ...]
        df = pd.DataFrame(raw, columns=["timestamp_ms", "open", "high", "low", "close"])
        df["date"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
        df["coin_id"] = coin_id
        df["ingested_at"] = datetime.now(timezone.utc)
        df = df.drop(columns=["timestamp_ms"])

        logger.info(f"CoinGecko OHLCV: {len(df)} candles for {coin_id}")
        return df

    def fetch_coin_metadata(self, coin_id: str = "bitcoin") -> dict:
        """
        Fetch coin metadata: description, links, genesis date, etc.
        Useful for enriching the mart layer.
        """
        self.rate_limit_sleep(self.RATE_LIMIT_SLEEP)
        data = self.get(f"/coins/{coin_id}", params={"localization": False})

        return {
            "coin_id": coin_id,
            "name": data["name"],
            "symbol": data["symbol"],
            "genesis_date": data.get("genesis_date"),
            "hashing_algorithm": data.get("hashing_algorithm"),
            "market_cap_rank": data["market_data"].get("market_cap_rank"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------ #
    #  Private helpers
    # ------------------------------------------------------------------ #

    def _parse_market_chart(self, raw: dict, coin_id: str) -> pd.DataFrame:
        """
        CoinGecko market_chart response has 3 lists:
          prices:       [[ts_ms, price], ...]
          market_caps:  [[ts_ms, mkt_cap], ...]
          total_volumes:[[ts_ms, volume], ...]

        We merge on timestamp.
        """
        prices = pd.DataFrame(raw["prices"], columns=["timestamp_ms", "price_usd"])
        mkt_caps = pd.DataFrame(raw["market_caps"], columns=["timestamp_ms", "market_cap_usd"])
        volumes = pd.DataFrame(raw["total_volumes"], columns=["timestamp_ms", "volume_usd"])

        df = prices.merge(mkt_caps, on="timestamp_ms").merge(volumes, on="timestamp_ms")
        df["date"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
        df["coin_id"] = coin_id
        df["ingested_at"] = datetime.now(timezone.utc)
        df = df.drop(columns=["timestamp_ms"])

        # Reorder columns nicely
        return df[["date", "coin_id", "price_usd", "market_cap_usd", "volume_usd", "ingested_at"]]