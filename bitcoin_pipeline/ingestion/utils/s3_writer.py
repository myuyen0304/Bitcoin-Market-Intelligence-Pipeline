"""
S3 Writer Utility
-----------------
Handles writing DataFrames to S3 Bronze layer as Parquet.
Follows Hive-style partitioning: s3://bucket/layer/source/year=YYYY/month=MM/day=DD/
"""

import logging
from datetime import datetime, timezone
from io import BytesIO

import boto3
import pandas as pd

logger = logging.getLogger(__name__)


class S3Writer:
    def __init__(
        self,
        bucket: str,
        region: str = "ap-southeast-1",
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ):
        """Create an S3 (or S3-compatible, e.g. MinIO) Parquet writer.

        Args:
            bucket: Target bucket name.
            region: AWS region for the client.
            endpoint_url: Custom S3 endpoint. Set this to point at MinIO
                (e.g. ``http://minio:9000``); leave ``None`` for real AWS S3.
            aws_access_key_id: Access key; ``None`` falls back to boto3's default
                credential chain (env vars, IAM role, ...).
            aws_secret_access_key: Secret key; ``None`` falls back to the chain.
        """
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            region_name=region,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def write_parquet(
        self,
        df: pd.DataFrame,
        layer: str,          # bronze / silver / gold
        source: str,         # coingecko / binance / feargreed
        dataset: str,        # ohlcv / ticker / index
        date: datetime | None = None,
    ) -> str:
        """
        Write DataFrame to S3 as Parquet with Hive-style partitioning.

        Returns the full S3 path written.
        """
        if date is None:
            date = datetime.now(timezone.utc)

        # Hive partition path
        s3_key = (
            f"{layer}/{source}/{dataset}/"
            f"year={date.year}/month={date.month:02d}/day={date.day:02d}/"
            f"{dataset}_{date.strftime('%Y%m%d_%H%M%S')}.parquet"
        )

        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        self.client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
            Metadata={
                "source": source,
                "dataset": dataset,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "row_count": str(len(df)),
            },
        )

        full_path = f"s3://{self.bucket}/{s3_key}"
        logger.info(f"Written {len(df)} rows → {full_path}")
        return full_path