WITH source_table AS (
    SELECT * FROM {{ source('StarterPack', 'Schools')}}
),

sy AS (
  SELECT * FROM {{ ref('dim_CurrentStarterPackSchoolYear')}}
),

final AS (
  SELECT
    sy.SchoolYear,
    source_table.*
  FROM source_table
  CROSS JOIN sy
)

SELECT * FROM final