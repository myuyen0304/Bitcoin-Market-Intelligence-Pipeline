with source as (
    select *
    from read_parquet('{{ bronze_path("feargreed", "index") }}', union_by_name = true, hive_partitioning = true)
),

typed as (
    select
        cast(date as date) as date_day,
        cast(fear_greed_value as integer) as fear_greed_value,
        cast(sentiment_label as varchar) as sentiment_label,
        cast(sentiment_bucket as varchar) as sentiment_bucket,
        cast(ingested_at as timestamp) as ingested_at,
        cast(year as integer) as partition_year,
        cast(month as integer) as partition_month,
        cast(day as integer) as partition_day
    from source
    where fear_greed_value is not null
),

cleaned as (
    select *
    from typed
    qualify row_number() over (
        partition by date_day
        order by ingested_at desc
    ) = 1
)

select *
from cleaned
