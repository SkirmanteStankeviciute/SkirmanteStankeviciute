{{ 
    config(
        materialized="view"
    )
}}

select
    brand_id,
    brand,
    num_listings,
    num_purchases
from {{ source("app", "brands") }}