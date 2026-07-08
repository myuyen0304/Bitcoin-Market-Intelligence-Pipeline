"""Live-API smoke tests (integration — hit real external endpoints).

These confirm the three upstream sources are reachable and still return the
shape the fetchers expect. They call CoinGecko / Binance / Fear&Greed *live*, so
they are marked ``integration`` and DESELECTED by default (see ``pytest.ini``) —
they are flaky on CI runners and Binance geo-blocks GitHub's US IPs.

Run them on demand:
    pytest -m integration bitcoin_pipeline/tests/test_apis.py -v
"""

import pandas as pd
import pytest
import requests

pytestmark = pytest.mark.integration


def test_coingecko():
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {"vs_currency": "usd", "days": 7, "interval": "daily"}

    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    assert "prices" in data and data["prices"], "expected non-empty prices list"
    df = pd.DataFrame(data["prices"], columns=["timestamp_ms", "price_usd"])
    assert (df["price_usd"] > 0).all(), "prices must be positive"


def test_binance():
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": "BTCUSDT", "interval": "1d", "limit": 7}

    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    assert data, "expected non-empty klines list"
    # Binance kline: [open_time, open, high, low, close, volume, ...]
    latest = data[-1]
    assert float(latest[4]) > 0, "close price must be positive"
    assert float(latest[2]) >= float(latest[3]), "high must be >= low"


def test_fear_greed():
    url = "https://api.alternative.me/fng/"
    params = {"limit": 7, "format": "json"}

    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    entries = data["data"]
    assert entries, "expected non-empty data list"
    value = int(entries[0]["value"])
    assert 0 <= value <= 100, "fear & greed index is defined on 0-100"
