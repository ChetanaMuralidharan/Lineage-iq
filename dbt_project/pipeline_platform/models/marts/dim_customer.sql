with stg as (
    select * from {{ ref('stg_customer') }}
)
select
    -- columns here
    customer_key,
    customer_name,
    address,
    nation_key,
    phone,
    account_balance,
    market_segment
from stg