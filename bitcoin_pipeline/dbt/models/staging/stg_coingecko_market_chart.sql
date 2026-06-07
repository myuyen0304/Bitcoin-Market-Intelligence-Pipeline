with source as (
    select *
    from read_parquet('{{ bronze_path("coingecko", "market_chart") }}', union_by_name = true, hive_partitioning = true)
),

typed as (
    select
        cast(date as timestamp) as date_day,
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
    select *
    from typed
    qualify row_number() over (
        partition by coin_id, date_day
        order by ingested_at desc
    ) = 1
)

select *
from cleaned
