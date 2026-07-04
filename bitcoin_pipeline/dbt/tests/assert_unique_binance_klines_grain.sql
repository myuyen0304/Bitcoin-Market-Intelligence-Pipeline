-- Grain check: stg_binance_klines must have exactly one row per
-- (symbol, interval, open_time). Binance 1d and 1h candles are unioned, so the
-- grain includes `interval`. Proves the dedup collapses duplicate ingests.
-- Fails (returns rows) if any grain repeats.
select
    symbol,
    interval,
    open_time,
    count(*) as n_rows
from {{ ref('stg_binance_klines') }}
group by symbol, interval, open_time
having count(*) > 1
