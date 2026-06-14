with lineitem as (
    select * from {{ ref('stg_lineitem') }}
),
parts as (
    select part_key from {{ ref('dim_part') }}
),
suppliers as (
    select supplier_key from {{ ref('dim_supplier') }}
)
select
    l.order_key,
    l.part_key,
    l.supplier_key,
    l.line_number,
    l.quantity,
    l.extended_price,
    l.discount,
    l.tax,
    l.return_flag,
    l.line_status,
    l.ship_date,
    l.commit_date,
    l.receipt_date,
    l.ship_mode
from lineitem l
inner join parts p on l.part_key = p.part_key
inner join suppliers s on l.supplier_key = s.supplier_key