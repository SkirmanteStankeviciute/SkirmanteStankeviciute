{{ 
    config(
        materialized="view"
    )
}}

select
    *
from `logical-bolt-417313.test_sstankeviciute.samata`
