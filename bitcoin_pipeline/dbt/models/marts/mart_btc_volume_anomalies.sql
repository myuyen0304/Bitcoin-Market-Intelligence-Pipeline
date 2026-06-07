with price as (
    select *
    from {{ ref('mart_btc_price_analysis') }}
),

scored as (
    select
        date_day,
        symbol,
        close,
        volume,
        quote_volume,
        num_trades,
        daily_return_pct,
        fear_greed_value,
        sentiment_label,
        avg(volume) over (
            partition by symbol
            order by date_day
            rows between 29 preceding and current row
        ) as volume_ma30,
        stddev_samp(volume) over (
            partition by symbol
            order by date_day
            rows between 29 preceding and current row
        ) as volume_std30,
        latest_ingested_at
    from price
),

final as (
    select
        *,
        case
            when volume_std30 is null or volume_std30 = 0 then null
            else (volume - volume_ma30) / volume_std30
        end as volume_z_score,
        case
            when volume_std30 is not null and volume_std30 > 0 and (volume - volume_ma30) / volume_std30 >= 2 then true
            else false
        end as is_volume_anomaly
    from scored
)

select *
from final
