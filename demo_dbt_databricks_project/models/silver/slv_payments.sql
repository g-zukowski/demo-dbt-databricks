{{
  config(
    materialized = 'incremental',
    unique_key = 'payment_key',
    merge_exclude_columns = 'created_on'
  )
}}

with brz_payments as (
    select *
    from {{ ref('brz_payments') }}
    qualify row_number() over (partition by id order by file_modification_time desc, created_on desc) = 1
)
select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as payment_key,
    CAST(p.id as string) as payment_no,
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_key,
    p.payment_method as payment_method,
    p.amount / 100 as payment_amount,
    p.file_modification_time,
    current_timestamp() as created_on
from brz_payments p
where 1 = 1
{% if is_incremental() %}
  and p.file_modification_time >= (select coalesce(max(file_modification_time), timestamp '1900-01-01') from {{ this }} )
{% endif %}  
;