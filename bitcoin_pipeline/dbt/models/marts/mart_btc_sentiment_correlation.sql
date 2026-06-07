with price as (
    select *
    from {{ ref('mart_btc_price_analysis') }}
    where fear_greed_value is not null
),

lagged as (
    select
        date_day,
        symbol,
        close,
        daily_return_pct,
        fear_greed_value,
        sentiment_label,
        sentiment_bucket,
        fear_greed_value - lag(fear_greed_value) over (partition by symbol order by date_day) as sentiment_change,
        lead(daily_return_pct) over (partition by symbol order by date_day) as next_day_return_pct,
        corr(fear_greed_value, daily_return_pct) over (
            partition by symbol
            order by date_day
            rows between 29 preceding and current row
        ) as rolling_30d_sentiment_return_corr,
        latest_ingested_at
    from price
)

select *
from lagged
