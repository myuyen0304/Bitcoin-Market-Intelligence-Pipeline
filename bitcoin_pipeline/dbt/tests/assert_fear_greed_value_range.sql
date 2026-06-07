select *
from {{ ref('stg_fear_greed_index') }}
where fear_greed_value < 0
   or fear_greed_value > 100
