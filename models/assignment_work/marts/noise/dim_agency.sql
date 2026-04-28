with agency_types as (
    select distinct
        trim(cast(agency_name as string)) as agency_name,
        trim(cast(agency as string)) as agency_type
    from {{ ref('stg_nyc_311_noise') }}
    where agency is not null
),

agency_dimension as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'agency_name',
            'agency_type'
        ]) }} as agency_key,
        agency_name,
        agency_type
    from agency_types
)

select * from agency_dimension