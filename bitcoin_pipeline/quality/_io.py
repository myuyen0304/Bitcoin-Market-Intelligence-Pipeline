"""Storage-agnostic Bronze reader for the Great Expectations gate.

``write_bronze`` returns either a local filesystem path or an ``s3://`` URI
depending on ``settings.s3_endpoint_url``. The GE gate reads that exact path
back to validate it, so it must handle both without the callers branching.

Reading a single S3 object goes through boto3 directly here (into an in-memory
buffer) rather than ``S3Writer``: this is a read in a utility module, not a
write in a task function, so the "all writes go through S3Writer" convention
does not apply. Using boto3 (already a dependency) avoids pulling in ``s3fs``
just to read one Parquet file.
"""

from __future__ import annotations

import io
from urllib.parse import urlparse

import pandas as pd

# Settings resolve differently depending on how the package is imported:
#   - Airflow container: PYTHONPATH=bitcoin_pipeline  → `config.settings`
#   - host / `python -m bitcoin_pipeline...`          → `bitcoin_pipeline.config.settings`
try:  # pragma: no cover - import shim
    from config.settings import settings
except ModuleNotFoundError:  # pragma: no cover - import shim
    from bitcoin_pipeline.config.settings import settings


def _read_bronze(path: str) -> pd.DataFrame:
    """Read a Bronze Parquet file into a DataFrame, from local disk or S3/MinIO.

    Args:
        path: A local filesystem path or an ``s3://bucket/key`` URI, exactly as
            returned by ``write_bronze``.

    Returns:
        The Parquet contents as a pandas DataFrame.

    Raises:
        ValueError: If ``path`` is empty.
    """
    if not path:
        raise ValueError("Cannot read Bronze from an empty path")

    if path.startswith("s3://"):
        return _read_bronze_s3(path)
    return pd.read_parquet(path)


def _read_bronze_s3(uri: str) -> pd.DataFrame:
    """Read a single Parquet object from S3/MinIO into a DataFrame.

    Uses the same endpoint and credentials the pipeline writes Bronze with, so
    the reader points at whatever object store ``settings`` is configured for
    (MinIO locally, real S3 in the cloud).

    Args:
        uri: An ``s3://bucket/key`` URI.

    Returns:
        The Parquet contents as a pandas DataFrame.
    """
    import boto3

    parsed = urlparse(uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")

    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body))
