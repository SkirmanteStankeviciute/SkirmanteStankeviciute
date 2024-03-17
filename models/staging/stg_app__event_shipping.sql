{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "inserted_at",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

select
    timestamp_seconds(time) as event_time,
    shipping_id,
    transaction_id,
    carrier,
    event_type,
    current_date as inserted_at
from {{ source("app", "event_shipping") }} 

{% if is_incremental() %}
where
    current_date () > _dbt_max_partition
{% endif %}