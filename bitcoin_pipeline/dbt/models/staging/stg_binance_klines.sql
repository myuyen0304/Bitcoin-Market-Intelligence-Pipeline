with klines_1d as (
    select *
    from read_parquet('{{ bronze_path("binance", "klines_1d") }}', union_by_name = true, hive_partitioning = true)
),

klines_1h as (
    select *
    from read_parquet('{{ bronze_path("binance", "klines_1h") }}', union_by_name = true, hive_partitioning = true)
),

unioned as (
    select * from klines_1d
    union all
    select * from klines_1h
),

typed as (
    select
        cast(open_time as timestamp) as open_time,
        cast(close_time as timestamp) as close_time,
        cast(symbol as varchar) as symbol,
        cast(interval as varchar) as interval,
        cast(open as double) as open,
        cast(high as double) as high,
        cast(low as double) as low,
        cast(close as double) as close,
        cast(volume as double) as volume,
        cast(quote_volume as double) as quote_volume,
        cast(num_trades as integer) as num_trades,
        cast(taker_buy_base_vol as double) as taker_buy_base_vol,
        cast(taker_buy_quote_vol as double) as taker_buy_quote_vol,
        cast(ingested_at as timestamp) as ingested_at,
        cast(year as integer) as partition_year,
        cast(month as integer) as partition_month,
        cast(day as integer) as partition_day
    from unioned
    where symbol is not null
),

cleaned as (
    select *
    from typed
    qualify row_number() over (
        partition by symbol, interval, open_time
        order by ingested_at desc
    ) = 1
)

select *
from cleaned
