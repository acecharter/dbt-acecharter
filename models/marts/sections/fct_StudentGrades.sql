SELECT
  SchoolId,
  SessionName,
  SectionIdentifier,
  ClassPeriodName,
  StudentUniqueId,
  GradingPeriodDescriptor,
  GradeTypeDescriptor,
  IsCurrentGradingPeriod,
  NumericGradeEarned,
  LetterGradeEarned
FROM {{ ref('stg_SP__CourseGrades') }}