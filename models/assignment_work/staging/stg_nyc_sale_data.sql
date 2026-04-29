-- Clean and standardize sales data
-- One row per service request

WITH source AS (
   SELECT * FROM {{ source('raw', 'source_sale_data') }}
), -- Easier to refer to the dbt reference to a long name table this way

cleaned AS (
   SELECT
       -- Get all columns from source, except ones we're transforming below
       -- To do cleaning on them or explicitly cast them as types just in case
       * EXCEPT (
           sale_date,
           sale_price,
           zip_code,
           borough,
           address,
           latitude,
           longitude,
           apartment_number,
           neighborhood,
           community_board,
           building_class_as_of_final,
           tax_class_at_time_of_sale,
           building_class_category,
           tax_class_as_of_final_roll,
           land_square_feet,
           gross_square_feet,
           residential_units,
           commercial_units,
           total_units
       ),

       -- Identifiers
       CAST(sale_date AS STRING) AS service_request_id,
       CAST(sale_price AS NUMERIC) AS sale_price,

       -- Date/Time
       CAST(sale_date AS TIMESTAMP) AS sale_date,
       
       -- Location - clean zip code, handling several common zip code data problems
       CASE
           WHEN UPPER(TRIM(CAST(zip_code AS STRING))) IN ('N/A', 'NA') THEN NULL
           WHEN UPPER(TRIM(CAST(zip_code AS STRING))) = 'ANONYMOUS' THEN 'Anonymous'
           WHEN LENGTH(CAST(zip_code AS STRING)) = 5 THEN CAST(zip_code AS STRING)
           WHEN LENGTH(CAST(zip_code AS STRING)) = 9 THEN CAST(zip_code AS STRING)
           WHEN LENGTH(CAST(zip_code AS STRING)) = 10
               AND REGEXP_CONTAINS(CAST(zip_code AS STRING), r'^\d{5}-\d{4}')
           THEN CAST(zip_code AS STRING)
           ELSE NULL
       END AS zip_code,

       -- Location - standardized borough, just in case
       CASE
           WHEN UPPER(TRIM(borough)) = '1' THEN 'Manhattan'
           WHEN UPPER(TRIM(borough)) = '2' THEN 'Bronx'
           WHEN UPPER(TRIM(borough)) = '3' THEN 'Brooklyn'
           WHEN UPPER(TRIM(borough)) = '4' THEN 'Queens'
           WHEN UPPER(TRIM(borough)) = '5' THEN 'Staten Island'
           ELSE 'UNKNOWN or CITYWIDE'
       END AS borough,

       CAST(address AS STRING) AS address,
       CAST(latitude AS DECIMAL) AS latitude,
       CAST(longitude AS DECIMAL) AS longitude,
       CAST(apartment_number AS STRING) AS apartment_number,
       CAST(neighborhood AS STRING) AS neighborhood,
       CAST(community_board AS STRING) AS community_board,

       -- Property details
       CAST(building_class_as_of_final AS STRING) AS building_class_as_of_final,
       CAST(tax_class_at_time_of_sale AS STRING) AS tax_class_at_time_of_sale,
       CAST(building_class_category AS STRING) AS building_class_category,
       CAST(tax_class_as_of_final_roll AS STRING) AS tax_class_as_of_final_roll,

       CAST(land_square_feet AS NUMERIC) AS land_square_feet,
       CAST(gross_square_feet AS NUMERIC) AS gross_square_feet,

       CAST(residential_units AS NUMERIC) AS residential_units,
       CAST(commercial_units AS NUMERIC) AS commercial_units,       
       CAST(total_units AS NUMERIC) AS total_units, 

   -- Metadata
   CURRENT_TIMESTAMP() AS _stg_loaded_at

FROM source

   -- Filters
   WHERE sale_date IS NOT NULL
   AND sale_price IS NOT NULL
   AND borough IS NOT NULL

)

SELECT * FROM cleaned
-- All should be part of this table: stg_nyc_sale_data