"""
Binance Fetcher
---------------
Two modes:
  1. REST (batch)    — fetch historical klines (OHLCV) for any time range
  2. WebSocket       — real-time price tick stream → Kafka producer

Public endpoints, no API key needed for market data.

Usage (batch):
    fetcher = BinanceFetcher()
    df = fetcher.fetch(symbol="BTCUSDT", interval="1d", start="2022-01-01")

Usage (streaming → Kafka):
    fetcher = BinanceFetcher()
    fetcher.stream_to_kafka(symbol="BTCUSDT", kafka_topic="btc-ticks")
"""

import json
import logging
import time
from datetime import datetime, timezone

import pandas as pd
import websocket

from ingestion.utils.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)


class BinanceFetcher(BaseFetcher):
    BASE_URL = "https://api.binance.com/api/v3"

    # Binance free limits: 1200 weight/min; klines = 2 weight per call
    KLINES_LIMIT = 1000      # max rows per request
    RATE_LIMIT_SLEEP = 0.5   # safe for public endpoints

    # Interval map — used for validation
    VALID_INTERVALS = {
        "1m", "3m", "5m", "15m", "30m",
        "1h", "2h", "4h", "6h", "12h",
        "1d", "3d", "1w", "1M",
    }

    def fetch(
        self,
        symbol: str = "BTCUSDT",
        interval: str = "1d",
        start: str | None = None,   # "2022-01-01" or None (last 365 days)
        end: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch historical klines (OHLCV) from Binance REST API.
        Handles pagination automatically — Binance returns max 1000 rows/call.

        Args:
            symbol:   Trading pair, e.g. "BTCUSDT"
            interval: Kline interval, e.g. "1d", "1h", "4h"
            start:    Start date string "YYYY-MM-DD" (UTC)
            end:      End date string "YYYY-MM-DD" (UTC), defaults to now
        """
        if interval not in self.VALID_INTERVALS:
            raise ValueError(f"Invalid interval '{interval}'. Valid: {self.VALID_INTERVALS}")

        start_ms = self._to_ms(start) if start else None
        end_ms = self._to_ms(end) if end else int(time.time() * 1000)

        logger.info(f"Fetching Binance klines | {symbol} {interval} from {start} to {end}")

        all_klines: list[list] = []
        current_start = start_ms

        while True:
            params = {
                "symbol": symbol,
                "interval": interval,
                "limit": self.KLINES_LIMIT,
            }
            if current_start:
                params["startTime"] = current_start
            if end_ms:
                params["endTime"] = end_ms

            batch = self.get("/klines", params=params)

            if not batch:
                break

            all_klines.extend(batch)
            logger.debug(f"Batch fetched: {len(batch)} rows | total: {len(all_klines)}")

            # Binance pagination: next batch starts after last candle's close time
            last_close_time = batch[-1][6]  # index 6 = close time ms
            if len(batch) < self.KLINES_LIMIT or last_close_time >= end_ms:
                break

            current_start = last_close_time + 1
            self.rate_limit_sleep(self.RATE_LIMIT_SLEEP)

        df = self._parse_klines(all_klines, symbol, interval)
        logger.info(f"Binance: {len(df)} klines for {symbol} {interval}")
        return df

    def stream_to_kafka(
        self,
        symbol: str = "BTCUSDT",
        kafka_topic: str = "btc-price-ticks",
        kafka_bootstrap: str = "localhost:9092",
    ) -> None:
        """
        Subscribe to Binance WebSocket trade stream and produce to Kafka.
        Runs indefinitely — wrap in a separate process or thread.

        Message format to Kafka:
        {
            "symbol": "BTCUSDT",
            "price": 67432.15,
            "quantity": 0.002,
            "event_time": "2024-01-15T10:23:45Z",
            "trade_id": 123456789
        }
        """
        # Import here — kafka-python is optional for batch-only setups
        try:
            from kafka import KafkaProducer
        except ImportError:
            raise ImportError("Install kafka-python: pip install kafka-python")

        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )

        stream_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
        logger.info(f"Connecting to Binance WebSocket: {stream_url}")

        def on_message(ws, message):
            data = json.loads(message)
            record = {
                "symbol": data["s"],
                "price": float(data["p"]),
                "quantity": float(data["q"]),
                "event_time": datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc).isoformat(),
                "trade_id": data["t"],
                "is_buyer_maker": data["m"],
            }
            producer.send(
                kafka_topic,
                key=symbol,
                value=record,
            )
            logger.debug(f"Produced: {record['symbol']} @ {record['price']}")

        def on_error(ws, error):
            logger.error(f"WebSocket error: {error}")

        def on_close(ws, close_status_code, close_msg):
            logger.warning(f"WebSocket closed: {close_status_code} | {close_msg}")
            producer.flush()

        ws_app = websocket.WebSocketApp(
            stream_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws_app.run_forever(ping_interval=30, ping_timeout=10)

    # ------------------------------------------------------------------ #
    #  Private helpers
    # ------------------------------------------------------------------ #

    def _parse_klines(self, raw: list[list], symbol: str, interval: str) -> pd.DataFrame:
        """
        Binance kline format (12 fields per row):
        [0]  open_time, [1] open, [2] high, [3] low, [4] close,
        [5]  volume, [6] close_time, [7] quote_asset_volume,
        [8]  num_trades, [9] taker_buy_base_vol, [10] taker_buy_quote_vol, [11] ignore
        """
        df = pd.DataFrame(raw, columns=[
            "open_time_ms", "open", "high", "low", "close",
            "volume", "close_time_ms", "quote_volume",
            "num_trades", "taker_buy_base_vol", "taker_buy_quote_vol", "_ignore",
        ])

        # Type casting
        for col in ["open", "high", "low", "close", "volume", "quote_volume",
                    "taker_buy_base_vol", "taker_buy_quote_vol"]:
            df[col] = df[col].astype(float)

        df["num_trades"] = df["num_trades"].astype(int)
        df["open_time"] = pd.to_datetime(df["open_time_ms"], unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time_ms"], unit="ms", utc=True)
        df["symbol"] = symbol
        df["interval"] = interval
        df["ingested_at"] = datetime.now(timezone.utc)

        return df[[
            "open_time", "close_time", "symbol", "interval",
            "open", "high", "low", "close",
            "volume", "quote_volume", "num_trades",
            "taker_buy_base_vol", "taker_buy_quote_vol",
            "ingested_at",
        ]]

    @staticmethod
    def _to_ms(date_str: str) -> int:
        """Convert 'YYYY-MM-DD' string to Unix milliseconds (UTC)."""
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)