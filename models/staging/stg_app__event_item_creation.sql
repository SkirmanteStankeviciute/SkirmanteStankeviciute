{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "inserted_at",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

select
    timestamp_seconds(time) as event_time,
    item_id,
    user_id,
    platform,
    initial_item_price,
    package_size_code,
    brand_id,
    category,
    status,
    size,
    current_date() as inserted_at
from {{ source("app", "event_item_creation") }} 

{% if is_incremental() %}
where
    current_date () > _dbt_max_partition
{% endif %}