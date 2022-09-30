WITH source_table AS(
  SELECT * FROM {{ source('StarterPack', 'CourseEnrollments_v2')}}
),

sy AS (
  SELECT * FROM {{ ref('dim_CurrentSchoolYear')}}
),

final AS (
  SELECT
    sy.SchoolYear,
    source_table.*
  FROM source_table
  CROSS JOIN sy
)

SELECT * FROM final