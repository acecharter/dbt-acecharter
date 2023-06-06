WITH
  grades AS (
      SELECT * FROM {{ ref('stg_SP__CourseGrades') }}
      UNION ALL
      SELECT * FROM {{ ref('stg_SPA__CourseGrades_SY22') }}
  ),

  final AS (
    SELECT
      SchoolYear,
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
      IsCurrentCourseEnrollment,
      IsCurrentGradingPeriod,
      NumericGradeEarned,
      LetterGradeEarned
    FROM grades
    WHERE
      (GradeTypeDescriptor IN ('Final', 'Grading Period'))
      OR (IsCurrentCourseEnrollment = TRUE AND IsCurrentGradingPeriod = TRUE)
  )

SELECT * FROM final
