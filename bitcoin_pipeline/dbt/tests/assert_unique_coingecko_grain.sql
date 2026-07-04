-- Grain check: stg_coingecko_market_chart must have exactly one row per
-- (coin_id, date_day). This proves the `qualify row_number()` dedup in the model
-- actually collapses duplicate ingests. Fails (returns rows) if any grain repeats.
select
    coin_id,
    date_day,
    count(*) as n_rows
from {{ ref('stg_coingecko_market_chart') }}
group by coin_id, date_day
having count(*) > 1
