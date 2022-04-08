SELECT
  AssessmentId,
  AceAssessmentId,
  StudentRenaissanceID,
  StudentIdentifier,
  StateUniqueId,
  TestedSchoolId,
  SchoolYear,
  AssessmentDate,
  GradeLevel,
  GradePlacement
FROM {{ ref('int_RenStar__unioned') }}