"""
S3 Writer Utility
-----------------
Handles writing DataFrames to S3 Bronze layer as Parquet.
Follows Hive-style partitioning: s3://bucket/layer/source/year=YYYY/month=MM/day=DD/
"""

import logging
from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd

logger = logging.getLogger(__name__)


class S3Writer:
    def __init__(self, bucket: str, region: str = "ap-southeast-1"):
        self.bucket = bucket
        self.client = boto3.client("s3", region_name=region)

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
            date = datetime.utcnow()

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
                "ingested_at": datetime.utcnow().isoformat(),
                "row_count": str(len(df)),
            },
        )

        full_path = f"s3://{self.bucket}/{s3_key}"
        logger.info(f"Written {len(df)} rows → {full_path}")
        return full_path