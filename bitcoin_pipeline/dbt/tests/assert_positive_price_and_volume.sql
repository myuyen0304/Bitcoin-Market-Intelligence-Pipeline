select *
from {{ ref('stg_binance_klines') }}
where close <= 0
   or volume < 0
   or quote_volume < 0
