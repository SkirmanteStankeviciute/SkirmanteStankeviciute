{{
    config(
        materialized="table"
    )
}}


select
    routes.event_time,
    routes.shipping_id,
    routes.transaction_id,
    routes.carrier,
    routes.sold_item,
    routes.buyer_id,
    routes.buyers_country,
    routes.item_id,
    routes.sale_price,
    routes.package_size_code,
    routes.brand_id,
    shipping_time.days_taken,
from {{ ref("int_app__shipping_routes") }} as routes
left join {{ ref("int_app__shipping_time") }} as shipping_time using (shipping_id, transaction_id)

