with enriched as (
    select *
    from {{ ref('int_btc_daily_enriched') }}
),

base_metrics as (
    select
        date_day,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        quote_volume,
        num_trades,
        market_cap_usd,
        coingecko_volume_usd,
        fear_greed_value,
        sentiment_label,
        sentiment_bucket,
        close - lag(close) over (partition by symbol order by date_day) as price_change_usd,
        100 * (close / nullif(lag(close) over (partition by symbol order by date_day), 0) - 1) as daily_return_pct,
        latest_ingested_at
    from enriched
),

metrics as (
    select
        *,
        avg(close) over (
            partition by symbol
            order by date_day
            rows between 6 preceding and current row
        ) as ma7_close,
        avg(close) over (
            partition by symbol
            order by date_day
            rows between 29 preceding and current row
        ) as ma30_close,
        stddev_samp(daily_return_pct) over (
            partition by symbol
            order by date_day
            rows between 29 preceding and current row
        ) as volatility_30d_pct
    from base_metrics
)

select *
from metrics
