{{
    config(
        materialized="table"
    )
}}

select
    event_time,
    transaction_id,
    item_id,
    buyer_id,
    bargaining_flag,
    sale_price,
    initial_item_price,
    category,
    package_size_code,
    brand
from {{ ref("int_app_bargaining") }}