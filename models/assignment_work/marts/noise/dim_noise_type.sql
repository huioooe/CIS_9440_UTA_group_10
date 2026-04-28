-- Noise type dimension for 311 reports
WITH noise_types AS (
    SELECT DISTINCT
      problem_type, 
      ARRAY_TO_STRING([descriptor, descriptor_2], ",") AS descriptor,
      method_of_submission AS source_type

   FROM {{ ref('stg_nyc_311_noise') }}
   WHERE problem_type IS NOT NULL

),

noise_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key([    
           'problem_type',
           'descriptor',
           'source_type'
       ]) }} AS noise_type_key, -- TODO: figure out how to generate int surrogate key
       problem_type,
       descriptor,
       source_type

   FROM noise_types
)

SELECT * FROM noise_dimension