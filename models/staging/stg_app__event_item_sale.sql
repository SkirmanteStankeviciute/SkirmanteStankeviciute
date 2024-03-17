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
    transaction_id,
    successful_transaction,
    item_id,
    buyer_id,
    sale_price,
    current_date as inserted_at
from {{ source("app", "event_item_sale") }} 

{% if is_incremental() %}
where
    current_date () > _dbt_max_partition
{% endif %}
