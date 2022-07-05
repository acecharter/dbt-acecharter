SELECT DISTINCT
  SchoolYear,
  TestedSchoolId,
  StudentIdentifier AS StudentUniqueId,
  AceAssessmentId,
  AssessmentName,
  StarTestingWindow
FROM {{ref('int_RenStar__1_unioned')}}
WHERE
    AssessmentType LIKE '%Enterprise%'
    AND StudentIdentifier IS NOT NULL