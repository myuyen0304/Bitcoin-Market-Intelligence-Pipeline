{#
    Resolve the Bronze parquet glob for a source/dataset.

    Config-driven, mirroring the Python write side (ingestion/utils/bronze_writer.py):
      - S3_ENDPOINT_URL set   -> read from MinIO/S3  (s3://{bucket}/bronze/...)
      - S3_ENDPOINT_URL unset -> read from local disk ({DATA_DIR}/bronze/...)

    The path is rendered when dbt compiles the model (at run time, inside the dbt
    subprocess), so it always reflects the runtime environment.
#}
{% macro bronze_path(source, dataset) -%}
    {%- set endpoint = env_var('S3_ENDPOINT_URL', '') -%}
    {%- if endpoint -%}
        {%- set bucket = env_var('S3_BUCKET', 'bitcoin-pipeline-bronze') -%}
        s3://{{ bucket }}/bronze/{{ source }}/{{ dataset }}/**/*.parquet
    {%- else -%}
        {%- set data_dir = env_var('DATA_DIR', 'bitcoin_pipeline/data').replace('\\', '/') -%}
        {{ data_dir }}/bronze/{{ source }}/{{ dataset }}/**/*.parquet
    {%- endif -%}
{%- endmacro %}
