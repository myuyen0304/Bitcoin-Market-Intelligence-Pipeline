"""Runtime settings for local and S3-compatible pipeline execution."""

from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def _path_from_env(name: str, default: str) -> Path:
    """Return a path setting, resolving relative values from the project root."""
    value = os.getenv(name, default)
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


@dataclass(frozen=True)
class Settings:
    """Container for environment-driven pipeline settings."""

    data_dir: Path
    duckdb_path: Path
    s3_bucket: str
    aws_region: str
    s3_endpoint_url: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        """Build settings from environment variables with local-safe defaults."""
        return cls(
            data_dir=_path_from_env("DATA_DIR", "bitcoin_pipeline/data"),
            duckdb_path=_path_from_env("DUCKDB_PATH", "bitcoin_pipeline/data/bitcoin_pipeline.duckdb"),
            s3_bucket=os.getenv("S3_BUCKET", "bitcoin-pipeline-bronze"),
            aws_region=os.getenv("AWS_REGION", "ap-southeast-1"),
            s3_endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or None,
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or None,
        )


settings = Settings.from_env()
