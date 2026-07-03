# Airflow image for the Bitcoin pipeline.
#
# Two isolated Python environments live here on purpose:
#   1. Airflow's own env — gets only the lightweight ingestion runtime deps
#      (requests/pandas/pyarrow/...), which do not clash with Airflow's pins.
#   2. /opt/dbt-venv — a separate venv holding dbt, so dbt's dependencies can
#      never conflict with Airflow's. The DAG calls it via the DBT_BIN env var.
FROM apache/airflow:2.10.5-python3.11

# --- 1. Ingestion runtime deps into Airflow's environment ------------------
COPY docker/airflow/requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# --- 2. dbt in an isolated venv --------------------------------------------
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
        "dbt-core>=1.8,<2" "dbt-duckdb>=1.8,<2" "duckdb>=1.0,<2" \
    # Pre-install the DuckDB httpfs extension so dbt can read Bronze from MinIO/S3
    # at run time without a network round-trip (and offline).
    && /opt/dbt-venv/bin/python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')" \
    && chown -R airflow: /opt/dbt-venv
USER airflow
