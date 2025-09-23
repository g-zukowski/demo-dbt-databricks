with slv_customers as (
    select
        *
    from {{ ref('slv_customers') }}
),
slv_orders as (
    select
        *
    from {{ ref('slv_orders') }}
),
slv_payments as (
    select
        *
    from {{ ref('slv_payments') }}
),
customer_orders as (
    select
        customer_key,
        min(order_date) as first_order,
        max(order_date) as last_order,
        count(order_no) as number_of_orders
    from slv_orders
    group by customer_key
),
customer_payments as (
    select
        o.customer_key,
        sum(p.payment_amount) as payment_amount
    from slv_payments p
    left join slv_orders o on
         p.order_key = o.order_key
    group by o.customer_key
)
select
    c.customer_key as dim_customer_key,
    cast(date_format(co.first_order, 'yyyyMMdd') AS INT) as dim_first_order_day_key,
    cast(date_format(co.last_order, 'yyyyMMdd') AS INT) as dim_last_order_day_key,
    co.number_of_orders as kpi_number_of_orders,
    cp.payment_amount as kpi_customer_value
from slv_customers c
inner join customer_orders co
    on c.customer_key = co.customer_key
left join customer_payments cp
    on c.customer_key = cp.customer_key;