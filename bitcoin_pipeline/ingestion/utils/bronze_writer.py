"""Config-driven Bronze writer selector.

One entry point (:func:`write_bronze`) that both the Airflow DAG and the
standalone ingestion runner call, so nobody has to know *where* Bronze lives.

The target is chosen by a single switch — ``settings.s3_endpoint_url``:

    - unset (empty)  → local disk via :func:`write_local_parquet`
    - set            → MinIO / S3 via :class:`S3Writer`

Both writers produce the SAME Hive-partitioned layout so the dbt
``bronze_path`` macro can glob either one:

    bronze/{source}/{dataset}/year=YYYY/month=MM/day=DD/{dataset}_{ts}.parquet

Per the project convention, all S3 writes go through ``S3Writer`` — this module
never calls boto3 directly.
"""

import logging
from datetime import datetime

import pandas as pd

from config.settings import settings
from ingestion.utils.local_writer import write_local_parquet
from ingestion.utils.s3_writer import S3Writer

logger = logging.getLogger(__name__)


def write_bronze(
    df: pd.DataFrame,
    source: str,
    dataset: str,
    partition_date: datetime,
) -> str:
    """Write a DataFrame to the Bronze layer, on MinIO/S3 or local disk.

    The destination is decided by ``settings.s3_endpoint_url``: when it is set
    the data lands in the configured S3-compatible bucket, otherwise it lands on
    the local ``settings.data_dir`` tree. Callers stay identical either way.

    Args:
        df: The DataFrame to persist.
        source: Data source name (e.g. ``coingecko``, ``binance``, ``feargreed``).
        dataset: Dataset name within the source (e.g. ``market_chart``, ``klines_1d``).
        partition_date: Timestamp used for the ``year=/month=/day=`` partition
            path and the output filename.

    Returns:
        The full path written — an ``s3://`` URI for MinIO/S3, or a local
        filesystem path otherwise.
    """
    if settings.s3_endpoint_url:
        writer = S3Writer(
            bucket=settings.s3_bucket,
            region=settings.aws_region,
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
        return writer.write_parquet(
            df=df,
            layer="bronze",
            source=source,
            dataset=dataset,
            date=partition_date,
        )

    return str(
        write_local_parquet(
            df=df,
            source=source,
            dataset=dataset,
            base_dir=settings.data_dir,
            partition_date=partition_date,
        )
    )
