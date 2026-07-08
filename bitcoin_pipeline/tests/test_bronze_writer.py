"""Unit tests for the config-driven Bronze writer selector (no network, no S3).

``write_bronze`` is the single entry point every ingestion task uses. Its whole
job is *routing*: pick local disk vs S3/MinIO based on ``settings.s3_endpoint_url``
and preserve the identical Hive-partitioned layout either way. These tests pin
both branches — the local branch writes a real Parquet to a tmp dir, and the S3
branch is checked against a fake ``S3Writer`` so no boto3/MinIO is needed.
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from ingestion.utils import bronze_writer

_DF = pd.DataFrame({"price_usd": [42_000.0, 43_500.0], "coin_id": ["bitcoin", "bitcoin"]})
_DATE = datetime(2024, 3, 9, 14, 30, 5)


def _fake_settings(**overrides) -> SimpleNamespace:
    base = dict(
        s3_endpoint_url=None,
        s3_bucket="test-bucket",
        aws_region="ap-southeast-1",
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        data_dir=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_local_route_writes_hive_partitioned_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(
        bronze_writer, "settings", _fake_settings(s3_endpoint_url=None, data_dir=tmp_path)
    )

    path = bronze_writer.write_bronze(
        _DF, source="coingecko", dataset="market_chart", partition_date=_DATE
    )

    # Returns a real local file that exists and round-trips.
    assert not path.startswith("s3://")
    assert (tmp_path / "bronze").exists()
    written = pd.read_parquet(path)
    assert len(written) == 2

    # Hive layout: bronze/<source>/<dataset>/year=/month=/day=/<dataset>_<ts>.parquet
    rel = path.replace("\\", "/")
    assert "bronze/coingecko/market_chart/year=2024/month=03/day=09/" in rel
    assert rel.endswith("market_chart_20240309_143005.parquet")


def test_s3_route_used_when_endpoint_set(monkeypatch):
    captured = {}

    class FakeS3Writer:
        def __init__(self, **kwargs):
            captured["init"] = kwargs

        def write_parquet(self, **kwargs):
            captured["write"] = kwargs
            return "s3://test-bucket/bronze/binance/klines_1d/year=2024/.../f.parquet"

    monkeypatch.setattr(
        bronze_writer,
        "settings",
        _fake_settings(s3_endpoint_url="http://minio:9000"),
    )
    monkeypatch.setattr(bronze_writer, "S3Writer", FakeS3Writer)

    path = bronze_writer.write_bronze(
        _DF, source="binance", dataset="klines_1d", partition_date=_DATE
    )

    assert path.startswith("s3://test-bucket/")
    # Credentials/endpoint from settings are threaded into the writer.
    assert captured["init"]["endpoint_url"] == "http://minio:9000"
    assert captured["init"]["bucket"] == "test-bucket"
    # The DataFrame and partition metadata are forwarded unchanged.
    assert captured["write"]["layer"] == "bronze"
    assert captured["write"]["source"] == "binance"
    assert captured["write"]["dataset"] == "klines_1d"
    assert captured["write"]["date"] is _DATE


def test_s3_writer_not_constructed_on_local_route(tmp_path, monkeypatch):
    # Guard against accidental boto3 client creation when running purely local.
    def _boom(*_a, **_k):
        raise AssertionError("S3Writer must not be instantiated on the local route")

    monkeypatch.setattr(
        bronze_writer, "settings", _fake_settings(s3_endpoint_url=None, data_dir=tmp_path)
    )
    monkeypatch.setattr(bronze_writer, "S3Writer", _boom)

    bronze_writer.write_bronze(
        _DF, source="feargreed", dataset="index", partition_date=_DATE
    )  # must not raise


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
