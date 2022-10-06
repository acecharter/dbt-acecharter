SELECT
  SchoolYear,
  StarTestingWindow,
  TestedSchoolId AS SchoolId,
  StudentIdentifier AS StudentUniqueId,
  AceAssessmentId,
  AssessmentName,
  AssessmentType,
  COUNT(*) ResultCount
FROM {{ref('stg_RenaissanceStar')}}
WHERE StudentIdentifier IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7