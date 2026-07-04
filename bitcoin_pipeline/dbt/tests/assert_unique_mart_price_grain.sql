-- Grain check: mart_btc_price_analysis must have exactly one row per
-- (symbol, date_day). Window functions (moving averages, daily return) are only
-- correct on a clean daily grain, so any duplicate day here means the metrics are
-- computed over repeated rows. Fails (returns rows) if any (symbol, date_day) repeats.
-- See docs/bug-coingecko-grain-dedup-fanout.md.
select
    symbol,
    date_day,
    count(*) as n_rows
from {{ ref('mart_btc_price_analysis') }}
group by symbol, date_day
having count(*) > 1
