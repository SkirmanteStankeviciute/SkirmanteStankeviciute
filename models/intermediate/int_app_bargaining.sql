{{
    config(
        materialized="table"
    )
}}

select
    sales.event_time,
    sales.transaction_id,
    sales.item_id,
    sales.buyer_id,
    case
        when sales.sale_price < initial_item.initial_item_price then 1
        else null
    end as bargaining_flag,
    sales.sale_price,
    initial_item.initial_item_price,
    initial_item.category,
    initial_item.package_size_code,
    initial_item.brand_id,
    brand.brand
from {{ ref("stg_app__event_item_sale") }} as sales
left join {{ ref("stg_app__event_item_creation") }} as initial_item on sales.item_id = initial_item.item_id
left join {{ ref("stg_app__brands") }} as brand on initial_item.brand_id = brand.brand_id
where sales.sale_price <> initial_item.initial_item_price

