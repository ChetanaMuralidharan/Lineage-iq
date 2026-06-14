with source as (
    select * from {{ source('raw', 'PART') }}
),
renamed as (
    select
        P_PARTKEY as part_key,
        P_NAME as part_name,
        P_MANUFACTURER as manufacturer,
        P_BRAND as brand,
        P_TYPE as part_type,
        P_SIZE as part_size,
        P_CONTAINER as container,
        P_RETAILPRICE as retail_price,
        P_COMMENT as comment
    from source
)
select * from renamed