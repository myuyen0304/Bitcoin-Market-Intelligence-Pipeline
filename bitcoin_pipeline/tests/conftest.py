"""Pytest bootstrap for the unit-test suite.

The pipeline modules import each other as top-level packages (``ingestion``,
``config``, ``quality``) because ``bitcoin_pipeline/`` is the package root that
gets put on ``PYTHONPATH`` in the Airflow container and by the standalone
runners. Pytest, however, only adds the *test file's* directory to ``sys.path``,
so without this shim ``import ingestion`` fails during collection.

Adding ``bitcoin_pipeline/`` here makes the same imports resolve on the host,
so the tests exercise the code exactly as the DAG imports it.
"""

from __future__ import annotations

import sys
from pathlib import Path

# .../bitcoin_pipeline/tests/conftest.py  ->  parents[1] == bitcoin_pipeline/
BITCOIN_PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(BITCOIN_PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(BITCOIN_PIPELINE_ROOT))
