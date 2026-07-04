{{ config(severity='warn') }}
-- Freshness gate (warn, not error): the pipeline lags one day by design and can
-- have weekend/holiday gaps, so warn — don't fail — if the newest BTC price row is
-- more than 3 days old. Stands in for `dbt source freshness` (this project reads
-- Bronze parquet directly via a macro, so there is no dbt source() to attach
-- freshness to). Fails (returns a row) only when data is stale.
select
    max(date_day) as latest_day,
    current_date as today
from {{ ref('mart_btc_price_analysis') }}
having max(date_day) < current_date - interval 3 day
