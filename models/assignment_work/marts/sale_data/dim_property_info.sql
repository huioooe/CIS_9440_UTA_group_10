with source as (
    select distinct
        building_class_as_of_final,
        building_class_category
    from {{ ref('stg_nyc_sale_data') }}
    where building_class_as_of_final is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['building_class_as_of_final', 'building_class_category']) }} as property_type_id,
    building_class_as_of_final as building_class,
    building_class_category
from source