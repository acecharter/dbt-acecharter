WITH
  grades AS (
    SELECT
      SchoolId,
      SessionName,
      SectionIdentifier,
      ClassPeriodName,
      StudentUniqueId,
      GradingPeriodDescriptor,
      CASE
        WHEN GradingPeriodDescriptor = 'First Nine Weeks' THEN 'Q1'
        WHEN GradingPeriodDescriptor = 'Second Nine Weeks' THEN 'Q2'
        WHEN GradingPeriodDescriptor = 'Third Nine Weeks' THEN 'Q3'
        WHEN GradingPeriodDescriptor = 'Fourth Nine Weeks' THEN 'Q4'
        WHEN GradingPeriodDescriptor = 'First Semester' THEN 'S1'
        WHEN GradingPeriodDescriptor = 'Second Semester' THEN 'S2'
      END AS GradingPeriod,
      GradeTypeDescriptor,
      IsCurrentGradingPeriod,
      NumericGradeEarned,
      LetterGradeEarned
    FROM {{ ref('stg_SP__CourseGrades') }}
  ),

  final_or_current_grades AS (
    SELECT *
    FROM grades
    WHERE
      (GradeTypeDescriptor IN ('Final', 'Grading Period'))
      OR (IsCurrentGradingPeriod = TRUE)
  )

SELECT * FROM final_or_current_grades
