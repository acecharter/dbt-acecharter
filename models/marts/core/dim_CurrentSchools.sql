WITH sy AS (
  SELECT * FROM {{ ref('dim_CurrentSchoolYear')}}
),

schools AS (
  SELECT * FROM {{ ref('dim_Schools')}}
),

final AS (
  SELECT schools.*
  FROM sy
  LEFT JOIN schools
  USING (SchoolYear)
)

SELECT * FROM final