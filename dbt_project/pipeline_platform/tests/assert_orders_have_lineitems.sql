select o.order_key
from {{ ref('fact_orders') }} o
left join {{ ref('fact_lineitem') }} l on o.order_key = l.order_key
where l.order_key is NULL