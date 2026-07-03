"""
API Smoke Test
--------------
Run this BEFORE setting up Airflow/S3 to confirm all 3 data sources
are accessible and returning expected data.

Usage:
    pip install requests pandas
    python tests/test_apis.py
"""

import sys
import traceback

import pandas as pd
import requests


def test_coingecko():
    print("\n[1/3] Testing CoinGecko API...")
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {"vs_currency": "usd", "days": 7, "interval": "daily"}

    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    prices = data["prices"]
    print(f"  ✓ Got {len(prices)} days of price data")
    print(f"  ✓ Latest price: ${prices[-1][1]:,.2f}")
    print(f"  ✓ Keys in response: {list(data.keys())}")

    # Parse to DataFrame to confirm schema
    df = pd.DataFrame(prices, columns=["timestamp_ms", "price_usd"])
    df["date"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    print(f"  ✓ DataFrame shape: {df.shape}")
    print(f"  ✓ Date range: {df['date'].min().date()} → {df['date'].max().date()}")
    return True


def test_binance():
    print("\n[2/3] Testing Binance API...")
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": "BTCUSDT", "interval": "1d", "limit": 7}

    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    print(f"  ✓ Got {len(data)} klines")

    # Binance kline: [open_time, open, high, low, close, volume, ...]
    latest = data[-1]
    print(f"  ✓ Latest close: ${float(latest[4]):,.2f}")
    print(f"  ✓ Latest volume: {float(latest[5]):,.4f} BTC")

    df = pd.DataFrame(data, columns=[
        "open_time_ms", "open", "high", "low", "close",
        "volume", "close_time_ms", "quote_volume", "num_trades",
        "taker_buy_base", "taker_buy_quote", "_ignore",
    ])
    print(f"  ✓ DataFrame shape: {df.shape}")
    return True


def test_fear_greed():
    print("\n[3/3] Testing Fear & Greed Index API...")
    url = "https://api.alternative.me/fng/"
    params = {"limit": 7, "format": "json"}

    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    entries = data["data"]
    print(f"  ✓ Got {len(entries)} days")

    latest = entries[0]
    print(f"  ✓ Today's value: {latest['value']} ({latest['value_classification']})")

    df = pd.DataFrame(entries)
    df["date"] = pd.to_datetime(df["timestamp"].astype(int), unit="s", utc=True)
    print(f"  ✓ DataFrame shape: {df.shape}")
    print(f"  ✓ Columns: {list(df.columns)}")
    return True


def main():
    results = {}
    tests = [
        ("CoinGecko", test_coingecko),
        ("Binance", test_binance),
        ("Fear & Greed", test_fear_greed),
    ]

    for name, test_fn in tests:
        try:
            results[name] = test_fn()
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            traceback.print_exc()
            results[name] = False

    print("\n" + "=" * 40)
    print("SMOKE TEST RESULTS")
    print("=" * 40)
    all_passed = True
    for name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}  {name}")
        if not passed:
            all_passed = False

    print("=" * 40)
    if all_passed:
        print("All APIs reachable — ready to build pipeline!")
    else:
        print("Fix failing APIs before proceeding.")
        sys.exit(1)


if __name__ == "__main__":
    main()