{{ 
    config(
        materialized="view"
    )
}}

select
    user_id,
    birthday,
    country_code,
    city,
    default_currency,
    language,
    registration_type
from {{ source("app", "users") }}