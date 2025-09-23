{{
  config(
    materialized = 'incremental',
    unique_key = 'order_key',
    merge_exclude_columns = 'created_on'
  )
}}

with brz_orders as (
    select *
    from {{ ref('brz_orders') }}
    qualify row_number() over (partition by id order by file_modification_time desc, created_on desc) = 1
)
select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as order_key,
    CAST(o.id as string) as order_no,
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as customer_key,
    o.order_date as order_date,
    o.status as order_status,
    o.file_modification_time,
    current_timestamp() as created_on
from brz_orders o
where 1 = 1
{% if is_incremental() -%}
  and o.file_modification_time >= (select coalesce(max(file_modification_time), timestamp '1900-01-01') from {{ this }} )
{%- endif %}  
;