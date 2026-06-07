{% macro bronze_path(source, dataset) -%}
    {%- set data_dir = env_var('DATA_DIR', 'bitcoin_pipeline/data').replace('\\', '/') -%}
    {{ data_dir }}/bronze/{{ source }}/{{ dataset }}/**/*.parquet
{%- endmacro %}
