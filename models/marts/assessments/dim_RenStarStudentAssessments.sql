SELECT
  AssessmentId,
  AceAssessmentId,
  StudentRenaissanceID,
  StudentIdentifier,
  StateUniqueId,
  TestedSchoolId,
  SchoolYear,
  AssessmentDate,
  Grade AS AssessedGradeLevel,
  GradePlacement
FROM {{ ref('int_RenStar__unioned') }}