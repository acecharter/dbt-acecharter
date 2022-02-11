WITH
ace_school_codes AS (
  SELECT
    SchoolNameFull,
    SchoolNameMid,
    SchoolNameShort,
    StateSchoolCode
  FROM {{ ref('stg_GoogleSheetData__Schools')}}
),

caaspp_results AS (
  SELECT
    a.* EXCEPT(StateSchoolCode),
    c.*
  FROM ace_school_codes as a
  LEFT JOIN {{ ref('int_CaasppSantaClaraCounty_Years_unioned')}} AS c
  ON a.StateSchoolCode = c.SchoolCode
),

demographics AS (
  SELECT *
  FROM {{ ref('stg_RawData__CaasppStudentGroups')}}
)

SELECT
  c.SchoolCode AS SchoolId,
  c.SchoolNameFull,
  c.SchoolNameMid,
  c.SchoolNameShort,
  c.TestYear,
  c.SchoolYear,
  c.DemographicId,
  d.StudentGroup,
  d.DemographicName,
  c.GradeLevel,
  c.TestId,
  CASE
    WHEN c.TestId = '1' THEN 'ELA'
    WHEN c.TestId = '2' Then 'Math'
  END AS TestSubject,
  c.StudentsEnrolled,
  c.StudentsWithScores,
  ROUND(c.StudentsWithScores / c.StudentsEnrolled, 4) AS PctStudentsWithScores,
  c.MeanScaleScore,
  ROUND(c.PctStandardMetAndAbove * c.StudentsWithScores, 0) AS StudentsStandardMetAndAbove,
  c.PctStandardMetAndAbove
FROM caaspp_results AS c
LEFT JOIN demographics AS d
ON
  c.DemographicId = d.DemographicId
WHERE
  c.GradeLevel >= 5 AND
  c.DemographicId IN (
  '1',   --All Students
  '128', --Reported Disabilities
  '31',  --Economic disadvantaged
  '160', --EL (English learner)
  '78',  --Hispanic or Latino
  '204'  --Economically disadvantaged Hispanic or Latino
)