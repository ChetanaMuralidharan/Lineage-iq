select *
from {{ ref('fact_lineitem') }}
where extended_price < 0
   or discount < 0
   or tax < 0