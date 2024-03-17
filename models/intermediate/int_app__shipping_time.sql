{{
    config(
        materialized="table"
    )
}}

with
initiated as (
    select 
        shipping_id,
        transaction_id,
        event_time as initiated_time
    from {{ ref("stg_app__event_shipping") }} as start_shipping
 where event_type = "initiated"
),

completed as (
    select 
        shipping_id,
        transaction_id,
        event_time as completed_time
    from {{ ref("stg_app__event_shipping") }} as complete_shipping
 where event_type = "delivered"
)

select 
      initiated.shipping_id,
      initiated.transaction_id,
      initiated.initiated_time,
      completed.completed_time,
      timestamp_diff(completed.completed_time, initiated.initiated_time, day) as days_taken
from initiated 
left join completed using (transaction_id, shipping_id)