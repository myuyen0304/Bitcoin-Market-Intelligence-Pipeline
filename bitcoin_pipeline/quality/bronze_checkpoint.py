"""Run Great Expectations checks against a freshly-written Bronze Parquet file.

This is the engine behind the ``validate_bronze`` Airflow task. For each source
it reads the Parquet back (storage-agnostic), validates it against that source's
expectation suite, and returns whether every expectation passed. A failure lets
the DAG stop before dbt runs — the Bronze quality gate.

Why an **ephemeral** context (not file-backed): GE 1.x runs a "freshness" check
that compares in-memory checkpoints/validation-definitions against ones persisted
on disk. A file-backed context therefore raises ``CheckpointRelatedResources
FreshnessError`` on the *second* run (a new process rebuilds objects the first
run saved). An ephemeral context persists nothing, so every run is self-contained
and the gate is reliable across repeated DAG runs. Data Docs are still produced —
each source renders an HTML site into ``quality/gx/data_docs/<source>/``.

Run one source manually:
    python -m bitcoin_pipeline.quality.bronze_checkpoint feargreed <path-to.parquet>
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import great_expectations as gx
from great_expectations.checkpoint import UpdateDataDocsAction
from great_expectations.data_context.types.base import ProgressBarsConfig

try:  # import shim — see _io.py
    from quality._io import _read_bronze
    from quality.suites import build_suite
except ModuleNotFoundError:  # pragma: no cover
    from bitcoin_pipeline.quality._io import _read_bronze
    from bitcoin_pipeline.quality.suites import build_suite

logger = logging.getLogger(__name__)

# Where the HTML Data Docs land (one self-contained site per source). Defaults to
# quality/gx/data_docs/ next to this file so it resolves the same regardless of
# the process cwd; override with GE_DOCS_DIR. The dir must be writable.
GX_DOCS_ROOT = Path(os.getenv("GE_DOCS_DIR", str(Path(__file__).resolve().parent / "gx" / "data_docs")))

_DATASOURCE = "bronze_pandas"
_BATCH = "current"
_SITE = "bronze"


def _build_context(source: str) -> gx.data_context.AbstractDataContext:
    """Build a fresh ephemeral GE context with a per-source Data Docs site.

    Args:
        source: The Bronze source; its Data Docs land under a same-named subdir.

    Returns:
        An in-memory (ephemeral) DataContext — nothing is persisted, so repeated
        runs never hit GE's cross-run freshness check.
    """
    context = gx.get_context(mode="ephemeral")
    # The "Calculating Metrics" tqdm bars are noise in Airflow task logs.
    context.variables.progress_bars = ProgressBarsConfig(
        globally=False, metric_calculations=False
    )
    context.add_data_docs_site(
        site_name=_SITE,
        site_config={
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": str(GX_DOCS_ROOT / source),
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        },
    )
    return context


def run_bronze_checks(source: str, parquet_path: str) -> dict:
    """Validate a Bronze Parquet file against its expectation suite.

    Reads the file back (local or ``s3://``), validates the whole partition (the
    single day just written), and refreshes that source's Data Docs.

    Args:
        source: One of ``coingecko``, ``binance``, ``feargreed``.
        parquet_path: Path/URI the fetcher wrote, as pushed to XCom.

    Returns:
        ``{"source": source, "success": bool, "path": parquet_path}``.
    """
    df = _read_bronze(parquet_path)
    logger.info("GE: validating %s Bronze (%d rows) from %s", source, len(df), parquet_path)

    context = _build_context(source)
    datasource = context.data_sources.add_pandas(_DATASOURCE)
    asset = datasource.add_dataframe_asset(source)
    batch_definition = asset.add_batch_definition_whole_dataframe(_BATCH)

    suite = build_suite(source)
    context.suites.add(suite)
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{source}_bronze", data=batch_definition, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=f"{source}_bronze_ckpt",
            validation_definitions=[validation_definition],
            # Refresh the HTML Data Docs site for this source on every run.
            actions=[UpdateDataDocsAction(name="update_data_docs", site_names=[_SITE])],
        )
    )

    result = checkpoint.run(batch_parameters={"dataframe": df})
    logger.info("GE: %s Bronze validation %s", source, "PASS" if result.success else "FAIL")
    return {"source": source, "success": bool(result.success), "path": parquet_path}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if len(sys.argv) != 3:
        print("Usage: python -m bitcoin_pipeline.quality.bronze_checkpoint <source> <parquet_path>")
        raise SystemExit(2)
    outcome = run_bronze_checks(sys.argv[1], sys.argv[2])
    print(outcome)
    raise SystemExit(0 if outcome["success"] else 1)
