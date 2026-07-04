-- Required for dbt 1.8+ so the elementary package can override dbt's built-in
-- `test` materialization (this is how Elementary captures test results into its
-- own schema). `default` = the non-Snowflake path, which is what dbt-duckdb uses.
-- See docs/elementary-vs-great-expectations.md and Elementary quickstart.
{% materialization test, default %}
    {{ return(elementary.materialization_test_default()) }}
{% endmaterialization %}
