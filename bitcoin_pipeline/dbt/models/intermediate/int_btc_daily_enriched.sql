with daily_price as (
    select
        cast(open_time as date) as date_day,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        quote_volume,
        num_trades,
        ingested_at
    from {{ ref('stg_binance_klines') }}
    where interval = '1d'
),

market_chart as (
    select
        cast(date_day as date) as date_day,
        coin_id,
        price_usd as coingecko_price_usd,
        market_cap_usd,
        volume_usd as coingecko_volume_usd,
        ingested_at as coingecko_ingested_at
    from {{ ref('stg_coingecko_market_chart') }}
),

sentiment as (
    select
        date_day,
        fear_greed_value,
        sentiment_label,
        sentiment_bucket,
        ingested_at as sentiment_ingested_at
    from {{ ref('stg_fear_greed_index') }}
)

select
    p.date_day,
    p.symbol,
    p.open,
    p.high,
    p.low,
    p.close,
    p.volume,
    p.quote_volume,
    p.num_trades,
    m.coingecko_price_usd,
    m.market_cap_usd,
    m.coingecko_volume_usd,
    s.fear_greed_value,
    s.sentiment_label,
    s.sentiment_bucket,
    greatest(p.ingested_at, coalesce(m.coingecko_ingested_at, p.ingested_at), coalesce(s.sentiment_ingested_at, p.ingested_at)) as latest_ingested_at
from daily_price p
left join market_chart m
    on p.date_day = m.date_day
left join sentiment s
    on p.date_day = s.date_day
