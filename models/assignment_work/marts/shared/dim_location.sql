with source as (
    select distinct
        borough,
        zip_code
    from {{ ref('stg_nyc_sale_data') }}
    where borough is not null
        and zip_code is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['borough', 'zip_code']) }} as location_id,
    borough,
    zip_code
from source