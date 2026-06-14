with source as (
    select * from {{ source('raw', 'SUPPLIER') }}
),
renamed as (
    select
        S_SUPPLIERKEY as supplier_key,
        S_NAME as supplier_name,
        S_ADDRESS as address,
        S_NATIONKEY as nation_key,
        S_PHONE as phone,
        S_ACCOUNTBALANCE as account_balance,
        S_COMMENT as comment
    from source
)
select * from renamed