with stg as (
    select * from {{ ref('stg_part') }}
)
select
    -- columns here
    part_key,
    part_name,
    manufacturer,
    brand,
    part_type,
    part_size,
    container,
    retail_price


from stg