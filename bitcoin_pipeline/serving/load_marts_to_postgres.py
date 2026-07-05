"""Publish the Gold marts from DuckDB into a Postgres serving database.

Metabase (and most BI tools) have first-class Postgres drivers but no official
DuckDB driver, and a file-based DuckDB warehouse locks single-writer — a BI tool
holding the file open would break the next ``dbt run``. So instead of pointing
Metabase at DuckDB directly, we materialize the Gold marts into a dedicated
Postgres "serving" database that Metabase reads from. This is the same
serving-layer pattern used in production stacks.

The copy is done entirely through DuckDB's ``postgres`` extension, so no extra
Python driver (psycopg2/sqlalchemy) is required. DuckDB is opened read-only, so
this can run without contending for the pipeline's write lock.

Usage (from the repo root, with the serving Postgres up):
    docker compose -f docker-compose.metabase.yml up -d serving-db
    python -m bitcoin_pipeline.serving.load_marts_to_postgres
"""

from __future__ import annotations

import logging
import os

import duckdb

from bitcoin_pipeline.config.settings import settings

logger = logging.getLogger(__name__)

# Gold marts exposed to BI. Order does not matter — each is copied independently.
MARTS: tuple[str, ...] = (
    "mart_btc_price_analysis",
    "mart_btc_volume_anomalies",
    "mart_btc_sentiment_correlation",
)


def _pg_connection_string() -> str:
    """Build a libpq connection string for the serving Postgres from env vars.

    Defaults match the ``serving-db`` service in ``docker-compose.metabase.yml``.

    Returns:
        A ``key=value`` libpq string consumable by DuckDB's postgres extension.
    """
    host = os.getenv("SERVING_PG_HOST", "localhost")
    port = os.getenv("SERVING_PG_PORT", "5433")
    dbname = os.getenv("SERVING_PG_DB", "analytics")
    user = os.getenv("SERVING_PG_USER", "analytics")
    password = os.getenv("SERVING_PG_PASSWORD", "analytics")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def load_marts_to_postgres() -> None:
    """Copy every Gold mart from DuckDB into the Postgres serving database.

    Each mart is fully replaced (drop + create) so the load is idempotent and
    always reflects the latest dbt build. The target schema is ``public``.

    Raises:
        FileNotFoundError: If the DuckDB warehouse file does not exist.
        duckdb.Error: If the postgres attach or a copy statement fails.
    """
    if not settings.duckdb_path.exists():
        raise FileNotFoundError(
            f"DuckDB file not found: {settings.duckdb_path}. Run dbt before loading marts."
        )

    pg_conn = _pg_connection_string()
    logger.info("Connecting to DuckDB warehouse at %s (read-only)", settings.duckdb_path)

    with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
        # The DuckDB file is opened read-only (so we never contend for the
        # pipeline's write lock); the attached Postgres must stay writable.
        con.execute(f"ATTACH '{pg_conn}' AS pg (TYPE postgres, READ_ONLY false)")

        for mart in MARTS:
            row_count = con.execute(f"SELECT count(*) FROM main.{mart}").fetchone()[0]
            # Drop + create (rather than CREATE OR REPLACE) — the postgres scanner
            # supports plain CREATE TABLE ... AS, keeping the load idempotent.
            con.execute(f"DROP TABLE IF EXISTS pg.public.{mart}")
            con.execute(f"CREATE TABLE pg.public.{mart} AS SELECT * FROM main.{mart}")
            logger.info("Loaded %s → pg.public.%s (%d rows)", mart, mart, row_count)

    logger.info("Serving load complete: %d marts published to Postgres", len(MARTS))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    load_marts_to_postgres()
