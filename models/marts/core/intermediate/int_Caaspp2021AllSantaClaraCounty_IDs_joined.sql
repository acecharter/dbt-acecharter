WITH entities AS (
  SELECT 
    CountyCode,
    DistrictCode,
    SchoolCode,
    TestYear,
    TypeId,
    CountyName,
    DistrictName,
    SchoolName,
    ZipCode
  FROM {{ ref('stg_RawData__Caaspp2021Entities')}}
),

results AS (
  SELECT 
    TypeId,
    CountyCode,
    DistrictCode,
    SchoolCode,
    TestYear,
    TypeId,
    DemographicId,
    TestType,
    TotalTestedAtReportingLevel,
    TotalTestedWithScoresAtReportingLevel,
    GradeLevel,
    TestId,
    StudentsEnrolled,
    StudentsTested,
    MeanScaleScore,
    PctStandardExceeded,
    PctStandardMet,
    PctStandardMetAndAbove,
    PctStandardNearlyMet,
    PctStandardNotMet,
    StudentsWithScores,
    Area1PctAboveStandard,
    Area1PctNearStandard,
    Area1PctBelowStandard,
    Area2PctAboveStandard,
    Area2PctNearStandard,
    Area2PctBelowStandard,
    Area3PctAboveStandard,
    Area3PctNearStandard,
    Area3PctBelowStandard,
    Area4PctAboveStandard,
    Area4PctNearStandard,
    Area4PctBelowStandard
  FROM {{ ref('stg_RawData__Caaspp2021AllSantaClaraCounty')}}
),

student_groups AS (
  SELECT
    DemographicId,
    DemographicName,
    StudentGroup
  FROM {{ ref('stg_RawData__CaasppStudentGroups')}}
),

tests AS (
  SELECT
    TestId,
    TestName
  FROM {{ ref('stg_RawData__CaasppTests')}}
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