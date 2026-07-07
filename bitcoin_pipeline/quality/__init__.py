"""Great Expectations Bronze validation.

This package holds the Bronze-layer data-quality gate. It validates the raw
Parquet a fetcher just wrote *before* dbt is allowed to transform it, so bad
input fails fast at the source instead of silently corrupting Silver/Gold.

Two complementary quality layers:
  - **Great Expectations (this package)** validates the *input* at Bronze,
    before any transform runs.
  - **dbt tests** validate the *output* at Silver/Gold, after transforms.

The public entry point is :func:`bronze_checkpoint.run_bronze_checks`, called
by the ``validate_bronze`` task in ``btc_daily_ingestion``.
"""
