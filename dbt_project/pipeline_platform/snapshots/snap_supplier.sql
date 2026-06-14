{% snapshot snap_supplier %}
{{
  config(
    target_schema='DBT_DEV',
    unique_key='supplier_key',
    strategy='check',
    check_cols=['account_balance', 'address', 'phone']
  )
}}
select * from {{ ref('dim_supplier') }}
{% endsnapshot %}