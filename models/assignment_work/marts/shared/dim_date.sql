with sale_dates as (
    select distinct
        cast(sale_date as date) as full_date
    from {{ ref('stg_nyc_sale_data') }}
    where sale_date is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['full_date']) }} as date_key,
    full_date,
    extract(day from full_date) as day,
    extract(month from full_date) as month,
    extract(year from full_date) as year,
    format_date('%A', full_date) as day_of_week,
    extract(week from full_date) as week_number,
    case when extract(dayofweek from full_date) in (1, 7) then true else false end as is_weekend,
    extract(quarter from full_date) as quarter,
    case
        when extract(month from full_date) in (12, 1, 2) then 'Winter'
        when extract(month from full_date) in (3, 4, 5) then 'Spring'
        when extract(month from full_date) in (6, 7, 8) then 'Summer'
        else 'Fall'
    end as season
from sale_dates