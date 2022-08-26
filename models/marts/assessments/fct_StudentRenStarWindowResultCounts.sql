SELECT
  SchoolYear,
  StarTestingWindow,
  TestedSchoolId AS SchoolId,
  StudentIdentifier AS StudentUniqueId,
  AceAssessmentId,
  AssessmentName,
  AssessmentType,
  COUNT(*) ResultCount
FROM {{ref('int_RenStar__1_unioned')}}
WHERE StudentIdentifier IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7