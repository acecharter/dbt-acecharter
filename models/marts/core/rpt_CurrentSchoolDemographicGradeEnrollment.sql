WITH enrollment AS (
  SELECT *
  FROM {{ ref('fct_CurrentSchoolDemographicGradeEnrollment') }}
),

schools AS (
  SELECT
    SchoolYear,
    SchoolId,
    SchoolName,
    SchoolNameMid,
    SchoolNameShort
  FROM {{ref('dim_CurrentSchools')}}
),

final AS (
  SELECT
    s.* EXCEPT(SchoolId),
    e.* EXCEPT(SchoolYear, SchoolId)
  FROM enrollment AS e
  LEFT JOIN schools AS s
  ON
    s.SchoolYear = e.SchoolYear
    AND s.SchoolId = e.SchoolId
)

SELECT * FROM final
