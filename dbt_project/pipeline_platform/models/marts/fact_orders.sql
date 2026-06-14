with orders as (
    select * from {{ ref('stg_orders') }}
),
customers as (
    select customer_key from {{ ref('dim_customer') }}
)
select
    o.order_key,
    o.customer_key,
    o.order_status,
    o.total_price,
    o.order_date,
    o.order_priority,
    o.ship_priority
from orders o
inner join customers c on o.customer_key = c.customer_key