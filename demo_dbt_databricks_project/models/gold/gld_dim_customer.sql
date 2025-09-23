with slv_customers as (
    select *
    from {{ ref('slv_customers') }}
)
select c.customer_key as dim_customer_key,
       c.customer_no as dim_customer_no,
       c.customer_first_name as dim_customer_first_name,
       c.customer_last_name as dim_customer_last_name
  from slv_customers c;