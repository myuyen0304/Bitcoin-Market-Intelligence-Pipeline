-- Grain check: int_btc_daily_enriched must have exactly one row per
-- (symbol, date_day). This is the layer where the CoinGecko dedup bug surfaced as
-- fan-out (a grain-mismatched join multiplied rows), so guard the grain directly
-- here. Fails (returns rows) if any (symbol, date_day) repeats.
-- See docs/bug-coingecko-grain-dedup-fanout.md.
select
    symbol,
    date_day,
    count(*) as n_rows
from {{ ref('int_btc_daily_enriched') }}
group by symbol, date_day
having count(*) > 1
