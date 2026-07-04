with source as (
    select *
    from read_parquet('{{ bronze_path("coingecko", "market_chart") }}', union_by_name = true, hive_partitioning = true)
),

typed as (
    select
        -- Grain is CALENDAR DAY. CoinGecko `market_chart?days=2` returns intraday
        -- snapshots whose timestamp is just "whenever the DAG happened to run", not a
        -- regular hourly series. Casting to `date` (not `timestamp`) is what makes the
        -- dedup below collapse those snapshots to one row/day. `observed_at` keeps the
        -- original intraday timestamp only so `order by` can pick the LATEST point in
        -- the day (≈ closing price). See docs/bug-coingecko-grain-dedup-fanout.md.
        cast(date as date) as date_day,
        cast(date as timestamp) as observed_at,
        cast(coin_id as varchar) as coin_id,
        cast(price_usd as double) as price_usd,
        cast(market_cap_usd as double) as market_cap_usd,
        cast(volume_usd as double) as volume_usd,
        cast(ingested_at as timestamp) as ingested_at,
        cast(year as integer) as partition_year,
        cast(month as integer) as partition_month,
        cast(day as integer) as partition_day
    from source
    where price_usd is not null
),

cleaned as (
    select
        date_day,
        coin_id,
        price_usd,
        market_cap_usd,
        volume_usd,
        ingested_at,
        partition_year,
        partition_month,
        partition_day
    from typed
    qualify row_number() over (
        partition by coin_id, date_day
        order by observed_at desc, ingested_at desc
    ) = 1
)

select *
from cleaned
