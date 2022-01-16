WITH entities AS (
  SELECT * FROM {{ ref('int_CaasppEntities_Years_unioned')}}
),

results AS (
  SELECT * FROM {{ ref('int_CaasppSantaClaraCounty_Years_unioned')}}
),

student_groups AS (
  SELECT * FROM {{ ref('stg_RawData__CaasppStudentGroups')}}
),

tests AS (
  SELECT * FROM {{ ref('stg_RawData__CaasppTests')}}
)


SELECT
  e.*,
  t.TestName,
  s.DemographicId,
  s.DemographicName,
  s.StudentGroup,
  r.GradeLevel,
  r.StudentsEnrolled,
  r.StudentsWithScores,
  r.MeanScaleScore,
  r.PctStandardExceeded,
  r.PctStandardMet,
  r.PctStandardMetAndAbove,
  r.PctStandardNearlyMet,
  r.PctStandardNotMet,
  r.Area1PctAboveStandard,
  r.Area1PctNearStandard,
  r.Area1PctBelowStandard,
  r.Area2PctAboveStandard,
  r.Area2PctNearStandard,
  r.Area2PctBelowStandard,
  r.Area3PctAboveStandard,
  r.Area3PctNearStandard,
  r.Area3PctBelowStandard,
  r.Area4PctAboveStandard,
  r.Area4PctNearStandard,
  r.Area4PctBelowStandard
FROM results AS r
LEFT JOIN entities AS e
ON
  r.CountyCode = e.CountyCode AND
  r.DistrictCode = e.DistrictCode AND
  r.SchoolCode = e.SchoolCode
LEFT JOIN student_groups AS s
ON r.DemographicId = s.DemographicId
LEFT JOIN tests AS t
ON r.TestId = t.TestId