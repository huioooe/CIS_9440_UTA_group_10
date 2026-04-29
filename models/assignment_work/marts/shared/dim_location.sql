with sale_locations as (
    select distinct
        borough,
        zip_code
    from {{ ref('stg_nyc_sale_data') }}
    where borough is not null
        and zip_code is not null
),

noise_locations as (
    select distinct
        borough,
        incident_zip as zip_code
    from {{ ref('stg_nyc_311_noise') }}
    where borough is not null
        and incident_zip is not null
),

combined as (
    select * from sale_locations
    union distinct
    select * from noise_locations
)

select
    {{ dbt_utils.generate_surrogate_key(['borough', 'zip_code']) }} as location_id,
    borough,
    zip_code
from combined