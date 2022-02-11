WITH
schools AS (
  SELECT *
  FROM {{ ref('int_CaasppSantaClaraCounty_Years_unioned')}}
  WHERE SchoolCode IN (
    '0116814', --ACE Empower
    '0125617', --ACE High School
    '0129247', --ACE Esperanza
    '0131656', --ACE Inspire
    '6046197', --Lee Mathson Middle (ARUSD)
    '6047229', --Bridges Academy (FMSD)
    '6062103', --Muwekma Ohlone Middle (SJUSD)
    '4330031'  --Independence High (ESUHSD)
  )
),

districts AS (
  SELECT *
  FROM {{ ref('int_CaasppSantaClaraCounty_Years_unioned')}}
  WHERE
    SchoolCode = '0000000' AND
    DistrictCode IN (
      '10439', -- Santa Clara County Office of Education
      '69369', -- Alum Rock Union
      '69450', -- Franklin-McKinley
      '69666', -- San Jose Unified
      '69427'  -- East Side Union
    )
),

county AS (
  SELECT *
  FROM {{ ref('int_CaasppSantaClaraCounty_Years_unioned')}}
  WHERE CountyCode = '43' AND DistrictCode = '00000' -- Santa Clara County
),

comparisons AS (
  SELECT * FROM schools
  UNION ALL
  SELECT * FROM districts
  UNION ALL
  SELECT * FROM county
),

entities AS (
  SELECT *
  FROM {{ ref('stg_RawData__CaasppEntities')}}
),

demographics AS (
  SELECT *
  FROM {{ ref('stg_RawData__CaasppStudentGroups')}}
)

SELECT
  e.TypeId,
  e.CountyCode,
  e.DistrictCode,
  e.SchoolCode,
  e.CountyName,
  e.DistrictName,
  e.SchoolName,
  e.TestYear,
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
FROM comparisons AS c
LEFT JOIN entities AS e
ON
  c.CountyCode = e.CountyCode AND
  c.DistrictCode = e.DistrictCode AND
  c.SchoolCode = e.SchoolCode AND
  c.TestYear = e.TestYear
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