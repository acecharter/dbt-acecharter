WITH
  grades AS (
    SELECT * FROM {{ ref('stg_SP__CourseGrades') }}
  ),

  numeric_grades AS(
    SELECT
      SchoolId,
      SessionName,
      SectionIdentifier,
      ClassPeriodName,
      StudentUniqueId,
      GradingPeriodDescriptor,
      GradeTypeDescriptor,
      IsCurrentGradingPeriod,
      'Numeric Grade' AS GradeType,
      CAST(CASE 
        WHEN LetterGradeEarned IS NULL THEN NULL
        ELSE NumericGradeEarned
      END AS STRING) AS GradeEarned
    FROM grades
  ),

  letter_grades AS(
    SELECT
      SchoolId,
      SessionName,
      SectionIdentifier,
      ClassPeriodName,
      StudentUniqueId,
      GradingPeriodDescriptor,
      GradeTypeDescriptor,
      IsCurrentGradingPeriod,
      'Letter Grade' AS GradeType,
      LetterGradeEarned AS GradeEarned
    FROM grades
  ),

final AS(
  SELECT * FROM numeric_grades
  UNION ALL
  SELECT * FROM letter_grades
  WHERE GradeEarned IS NOT NULL

)

SELECT * FROM final
  