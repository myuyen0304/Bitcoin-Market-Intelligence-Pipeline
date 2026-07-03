"""Local Bronze Parquet writer.

Writes DataFrames to the local Bronze layer using Hive-style partitioning:

    {base_dir}/bronze/{source}/{dataset}/year=YYYY/month=MM/day=DD/

This is the local-disk counterpart of ``S3Writer`` and is shared by both the
standalone ingestion runner and the Airflow DAG so partition layout stays
identical everywhere. The S3-compatible writer (``s3_writer.S3Writer``) is used
once MinIO/AWS is available.
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def write_local_parquet(
    df: pd.DataFrame,
    source: str,
    dataset: str,
    base_dir: Path,
    partition_date: datetime,
) -> Path:
    """Write a DataFrame to local Bronze storage using Hive-style partitions.

    Args:
        df: The DataFrame to persist.
        source: Data source name (e.g. ``coingecko``, ``binance``, ``feargreed``).
        dataset: Dataset name within the source (e.g. ``market_chart``, ``klines_1d``).
        base_dir: Root data directory; the ``bronze/`` tree is created underneath it.
        partition_date: Timestamp used for the ``year=/month=/day=`` partition path
            and the output filename.

    Returns:
        The full path of the Parquet file written.
    """
    output_dir = (
        base_dir
        / "bronze"
        / source
        / dataset
        / f"year={partition_date.year}"
        / f"month={partition_date.month:02d}"
        / f"day={partition_date.day:02d}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{dataset}_{partition_date:%Y%m%d_%H%M%S}.parquet"
    df.to_parquet(output_path, index=False, engine="pyarrow")
    logger.info("Written %d rows -> %s", len(df), output_path)
    return output_path
