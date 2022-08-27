WITH sy AS (
  SELECT * FROM {{ ref('dim_CurrentStarterPackSchoolYear')}}
),

students AS (
  SELECT * FROM {{ ref('dim_Students')}}
),

final AS (
  SELECT students.*
  FROM sy
  LEFT JOIN students
  USING (SchoolYear)
  WHERE students.IsCurrentlyEnrolled = true

)

SELECT * FROM final