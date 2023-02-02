WITH
reading_english AS(
  SELECT * FROM {{ ref('base_RSA__Reading_SY21')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RSA__Reading_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RS__Reading_v2')}}
),

reading_spanish AS (
  SELECT * FROM {{ ref('base_RSA__ReadingSpanish_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RS__ReadingSpanish_v2')}}
),

math_english AS (
  SELECT * FROM {{ ref('base_RSA__Math_SY21')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RSA__Math_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RS__Math_v2')}}
),

math_spanish AS (
  SELECT * FROM {{ ref('base_RSA__MathSpanish_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RS__MathSpanish_v2')}}
),

early_literacy_english AS (
  SELECT * FROM {{ ref('base_RSA__EarlyLiteracy_SY21')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RSA__EarlyLiteracy_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RS__EarlyLiteracy_v2')}}
),

early_literacy_spanish AS (
  SELECT * FROM {{ ref('base_RSA__EarlyLiteracySpanish_v2_SY22')}}
  UNION ALL
  SELECT * FROM {{ ref('base_RS__EarlyLiteracySpanish_v2')}}
),

reading_all AS (
  SELECT
    CASE
      WHEN AssessmentType LIKE '%Enterprise%' THEN '11'
      WHEN AssessmentType = 'ProgressMonitoring' THEN '24'
    END AS AceAssessmentId,
    CASE
      WHEN AssessmentType LIKE '%Enterprise%' THEN 'Star Reading'
      WHEN AssessmentType = 'ProgressMonitoring' THEN 'Star Reading Progress Monitoring'
    END AS AssessmentName,
    'Reading' AS AssessmentSubject,
    *
  FROM reading_english

  UNION ALL

  SELECT
    '13' AS AceAssessmentId,
    'Star Reading (Spanish)' AS AssessmentName,
    'Reading (Spanish)' AS AssessmentSubject,
    *
  FROM reading_spanish
),

math_all AS (
  SELECT
    CASE
      WHEN AssessmentType LIKE '%Enterprise%' THEN '10'
      WHEN AssessmentType = 'ProgressMonitoring' THEN '23'
    END AS AceAssessmentId,
    CASE
      WHEN AssessmentType LIKE '%Enterprise%' THEN 'Star Math'
      WHEN AssessmentType = 'ProgressMonitoring' THEN 'Star Math Progress Monitoring'
    END AS AssessmentName,
    'Math' AS AssessmentSubject,
    *
  FROM math_english

  UNION ALL

  SELECT
    '12' AS AceAssessmentId,
    'Star Math (Spanish)' AS AssessmentName,
    'Math (Spanish)' AS AssessmentSubject,
    *
  FROM math_spanish
),

early_literacy_all AS (
  SELECT
    '21' AS AceAssessmentId,
    'Star Early Literacy' AS AssessmentName,
    'Early Literacy' AS AssessmentSubject,
    *
  FROM early_literacy_english

  UNION ALL
  
  SELECT
    '22' AS AceAssessmentId,
    'Star Early Literacy (Spanish)' AS AssessmentName,
    'Early Literacy (Spanish)' AS AssessmentSubject,
    *
  FROM early_literacy_english
),

reading AS (
  SELECT
    * EXCEPT(
      InstructionalReadingLevel, 
      Lexile
    ),
    InstructionalReadingLevel,
    Lexile,
    CAST(NULL AS STRING) AS Quantile,
    CAST(NULL AS STRING) AS LiteracyClassification
  FROM reading_all
),

math AS (
  SELECT
    * EXCEPT(Quantile),
    CAST(NULL AS STRING) AS InstructionalReadingLevel,
    CAST(NULL AS STRING) AS Lexile,
    Quantile,
    CAST(NULL AS STRING) AS LiteracyClassification
  FROM math_all
),

early_literacy AS (
  SELECT
    * EXCEPT(Lexile, LiteracyClassification),
    CAST(NULL AS STRING) AS InstructionalReadingLevel,
    Lexile,
    CAST(NULL AS STRING) AS Quantile,
    LiteracyClassification
  FROM early_literacy_all
),

unioned AS (
  SELECT * FROM reading
  UNION ALL
  SELECT * FROM math
  UNION ALL
  SELECT * FROM early_literacy
),

final AS (
  SELECT
    AceAssessmentId,
    AssessmentName,
    AssessmentSubject,
    TestedSchoolId,
    TestedSchoolName,
    SchoolYear,
    StudentRenaissanceID,
    StudentIdentifier,
    StateUniqueId,
    DisplayName,
    LastName,
    FirstName,
    MiddleName,
    Gender,
    BirthDate,
    CAST(GradeLevel AS INT64) AS GradeLevel,
    EnrollmentStatus,
    AssessmentID,
    AssessmentDate,
    AssessmentNumber,
    AssessmentType,
    TotalTimeInSeconds,
    GradePlacement,
    CAST(AssessmentGradeLevel AS INT64) AS AssessmentGradeLevel,
    GradeEquivalent,
    ScaledScore,
    UnifiedScore,
    PercentileRank,
    NormalCurveEquivalent,
    StudentGrowthPercentileFallFall,
    StudentGrowthPercentileFallSpring,
    StudentGrowthPercentileFallWinter,
    StudentGrowthPercentileSpringSpring,
    StudentGrowthPercentileWinterSpring,
    CurrentSGP,
    StateBenchmarkCategoryLevel,
    AceTestingWindowName,
    AceTestingWindowStartDate,
    AceTestingWindowEndDate,
    StarTestingWindow,
    InstructionalReadingLevel,
    Lexile,
    Quantile,
    LiteracyClassification,
    CASE
      WHEN GradeEquivalent = '> 12.9' THEN 13
      WHEN GradeEquivalent = '> 11' THEN 11.1
      WHEN GradeEquivalent = '> 10' THEN 10.1
      WHEN GradeEquivalent = '> 9' THEN 9.1
      WHEN GradeEquivalent = '< 1' THEN 0.9
      ELSE CAST(GradeEquivalent as FLOAT64)
    END AS GradEquivalentNumeric
  FROM unioned
)

SELECT * FROM final