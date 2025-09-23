{{
  config(
    materialized = 'incremental',
    unique_key = 'customer_key',
    merge_exclude_columns = 'created_on'
  )
}}

with brz_customers as (
    select *
    from {{ ref('brz_customers') }}
    qualify row_number() over (partition by id order by file_modification_time desc, created_on desc) = 1
)
select
    {{ dbt_utils.generate_surrogate_key(['id']) }} as customer_key,
    CAST(c.id as string) as customer_no,
    c.first_name as customer_first_name,
    c.last_name as customer_last_name,
    c.file_modification_time,
    current_timestamp() as created_on
from brz_customers c
where 1 = 1
{% if is_incremental() %}
  and c.file_modification_time >= (select coalesce(max(file_modification_time), timestamp '1900-01-01') from {{ this }} )
{% endif %}  
;