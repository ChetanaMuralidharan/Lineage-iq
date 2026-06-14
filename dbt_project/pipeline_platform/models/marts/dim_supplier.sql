with stg as (
    select * from {{ ref('stg_supplier') }}
)
select
    -- columns here
    supplier_key,
    supplier_name,
    address,
    nation_key,
    phone,
    account_balance


from stg