-- WRITE THIS 1st - Start: all data from staging for relevant data
  WITH requests AS (
      SELECT * FROM {{ ref('stg_nyc_311_noise') }}
  ),

  dim_date AS (
      SELECT date_key, full_date FROM {{ ref('dim_date') }}
  ),

  dim_location AS (
      SELECT location_id, borough, zip_code FROM {{ ref('dim_location') }}
  ),

  dim_noise AS (
        SELECT
            noise_type_key, problem_type, descriptor, source_type
        FROM {{ ref('dim_noise_type') }}
  ),

dim_agency_info AS (
        SELECT
            agency_key, agency_name, agency_type
        FROM {{ ref('dim_agency') }}
  ),

 -- WRITE this - the structure for this 3rd: final AS ( ... ) + see end of file as well
  final AS (
      SELECT
          -- Surrogate key, generated from unique id in data.
          {{ dbt_utils.generate_surrogate_key(['r.service_request_id'])}} AS complaint_key,

          -- Natural key, direct from staging data
          r.service_request_id,

          -- Foreign keys
          d_created.date_key AS created_date_key,
          d_closed.date_key AS closed_date_key,
          l.location_id AS location_id,
          n.noise_type_key AS noise_type_key,
          a.agency_key AS agency_key,

          -- Status and submission details
          r.status,
          r.method_of_submission,

          -- Request location details
          r.latitude,
          r.longitude,
          r.location_type,
          r.incident_address,
          r.incident_zip,
          r.borough,

          -- Measures: small calculations included in a fact table
          CASE
              WHEN r.closed_date IS NOT NULL
              THEN DATE_DIFF(CAST(r.closed_date AS DATE), CAST(r.created_date AS DATE), DAY)
              ELSE NULL
          END AS days_to_close,

          -- Flags, support easy fact queries (e.g. 'all requests that are closed...')
          CASE WHEN UPPER(r.status) = 'CLOSED' THEN TRUE
          ELSE FALSE END AS is_closed
    
      -- **** INSIDE that, WRITE THIS 4th, join by join:
      FROM requests r -- All staging data

      LEFT JOIN dim_date d_created -- Date dimension to get created date
          ON CAST(r.created_date AS DATE) = d_created.full_date -- Cast as date to match yyyy-mm-dd date format

      LEFT JOIN dim_date d_closed
          ON CAST(r.closed_date AS DATE) = d_closed.full_date -- Cast as date to match yyyy-mm-dd date format

      LEFT JOIN dim_location l
          ON r.borough = l.borough
          AND r.incident_zip = l.zip_code

      LEFT JOIN dim_noise n
          ON r.problem_type = n.problem_type
          AND r.descriptor = n.descriptor
          AND r.method_of_submission = n.source_type
      
      LEFT JOIN dim_agency_info a
          ON r.agency = a.agency_type
          AND r.agency_name = a.agency_name
  )
 -- Also WRITE THIS 3rd
  SELECT * FROM final