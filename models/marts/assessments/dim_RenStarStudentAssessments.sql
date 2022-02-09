SELECT
  StudentRenaissanceID,
  AceAssessmentId,  
  StudentIdentifier,
  StateUniqueId,
  TestedSchoolId,
  SchoolYear,
  AssessmentId,
  AssessmentDate,
  Grade AS AssessedGradeLevel,
  GradePlacement
FROM {{ ref('int_RenStar__unioned') }}