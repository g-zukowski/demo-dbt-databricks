with date_spine as
(
    {{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2018-01-01' as date)",
    end_date="cast('2018-12-31' as date)"
   )
}}
)
select
    cast(date_format(date_day, 'yyyyMMdd') as int) as dim_day_key,
    cast(date_day as date) as dim_day_dt,
    lpad(cast(month(date_day) as string), 2, '0') as dim_month_no,
    date_format(date_day, 'MMMM') as dim_month_nm,
    cast(year(date_day) as string) as dim_year_no,
    last_day(date_day) as dim_last_day_of_month_dt
from date_spine;