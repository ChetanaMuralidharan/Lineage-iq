{% snapshot snap_part %}
{{
  config(
    target_schema='DBT_DEV',
    unique_key='part_key',
    strategy='check',
    check_cols=['retail_price', 'brand', 'part_type']
  )
}}
select * from {{ ref('dim_part') }}
{% endsnapshot %}