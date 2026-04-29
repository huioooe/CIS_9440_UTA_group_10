with sales as (
    select * from {{ ref('stg_nyc_sale_data') }}
),

dim_loc as (
    select * from {{ ref('dim_location') }}
),

dim_dt as (
    select * from {{ ref('dim_date') }}
),

dim_prop as (
    select * from {{ ref('dim_property_info') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['s.bbl', 's.sale_date']) }} as transaction_id,
    s.address,
    l.location_id,
    p.property_type_id,
    d.date_key as sale_date_key,
    s.land_square_feet,
    s.gross_square_feet,
    s.sale_price,
    cast(s.sale_date as date) as sale_date,
    s.latitude,
    s.longitude,
    s.apartment_number,
    s.residential_units,
    s.commercial_units,
    s.total_units,
    s.year_built,
    s.neighborhood,
    s.community_board
from sales s
left join dim_loc l
    on s.borough = l.borough
    and s.zip_code = l.zip_code
left join dim_dt d
    on cast(s.sale_date as date) = d.full_date
left join dim_prop p
    on s.building_class_as_of_final = p.building_class
    and s.building_class_category = p.building_class_category
where s.sale_price is not null
    and s.sale_date is not null