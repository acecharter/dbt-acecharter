WITH enrollment AS (
  SELECT *
  FROM {{ ref('fct_CurrentSchoolDemographicGradeEnrollment') }}
),

schools AS (
  SELECT
    SchoolId,
    SchoolName,
    SchoolNameMid,
    SchoolNameShort
  FROM {{ref('dim_Schools')}}
),

final AS (
  SELECT
    s.* EXCEPT(SchoolId),
    e.* EXCEPT(SchoolId)
  FROM enrollment AS e
  LEFT JOIN schools AS s
  USING (SchoolId)
)


SELECT * FROM final