
{{
    config(
        materialized="table"
    )
}}

with deduplicated_shipping as (
    {{ dbt_utils.deduplicate(
        relation=ref("stg_app__event_shipping"),
        partition_by="shipping_id, transaction_id, carrier",
        order_by="event_time desc",
        )
    }}
)

select
    shipping.event_time,
    shipping.shipping_id,
    shipping.transaction_id,
    shipping.carrier,
    sales.item_id as sold_item,
    sales.buyer_id,
    users.country_code as buyers_country,
    sales.item_id,
    sales.sale_price,
    items.package_size_code,
    items.brand_id
from deduplicated_shipping as shipping
left join {{ ref("stg_app__event_item_sale") }} as sales on shipping.transaction_id = sales.transaction_id
left join {{ ref("stg_app__users") }} as users on sales.buyer_id = users.user_id
left join {{ ref("stg_app__event_item_creation") }} as items on sales.item_id = items.item_id
