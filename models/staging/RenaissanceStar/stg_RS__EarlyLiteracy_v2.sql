WITH assessment_ids AS (
  SELECT 
    AceAssessmentId,
    AssessmentNameShort AS AssessmentName
  FROM {{ ref('stg_GSD__Assessments') }}
  WHERE AssessmentNameShort = 'Star Early Literacy'
),

testing_windows AS (
  SELECT *
  FROM {{ ref('stg_GSD__RenStarTestingWindows') }}
),

star AS (
  SELECT
    CASE
      WHEN SchoolIdentifier='57b1f93e473b517136000009' THEN '116814'
      WHEN SchoolIdentifier='57b1f93e473b51713600000b' THEN '129247'
      WHEN SchoolIdentifier='57b1f93e473b517136000007' THEN '131656'
      WHEN SchoolIdentifier='061182013023' THEN '125617'
      ELSE '999999999'
    END AS TestedSchoolId,
    NameofInstitution AS TestedSchoolName,
    CONCAT(LEFT(SchoolYear, 5), RIGHT(SchoolYear, 2)) AS SchoolYear,
    StudentRenaissanceID,
    StudentIdentifier,
    StateUniqueId,
    CASE
      WHEN MiddleName IS NULL THEN CONCAT(LastSurname, ", ", FirstName)
      ELSE CONCAT(LastSurname, ", ", FirstName, " ", MiddleName)
    END AS DisplayName,
    LastSurname AS LastName,
    FirstName,
    MiddleName,
    Gender,
    DATE(Birthdate) AS BirthDate,
    Gradelevel AS GradeLevel,
    EnrollmentStatus,
    AssessmentID,
    DATE(CompletedDateLocal) AS AssessmentDate,
    CAST(AssessmentNumber AS INT64) AS AssessmentNumber,
    AssessmentType,
    TotalTimeInSeconds,
    GradePlacement,
    Grade AS AssessmentGradeLevel,
    GradeEquivalent,
    ScaledScore,
    UnifiedScore,
    PercentileRank,
    NormalCurveEquivalent,
    Lexile,
    LiteracyClassification,
    StudentGrowthPercentileFallFall,
    StudentGrowthPercentileFallSpring,
    StudentGrowthPercentileFallWinter,
    StudentGrowthPercentileSpringSpring,
    StudentGrowthPercentileWinterSpring,
    CurrentSGP,
    CAST(RIGHT(StateBenchmarkCategoryName, 1) AS INT64) AS StateBenchmarkCategoryLevel
  FROM {{ source('RenaissanceStar', 'EarlyLiteracy_v2')}}
),

final AS (
  SELECT
    a.*,
    s.*,
    CASE WHEN s.AssessmentDate BETWEEN t.AceWindowStartDate AND t.AceWindowEndDate THEN t.TestingWindow END AS AceTestingWindowName,
    CASE WHEN s.AssessmentDate BETWEEN t.AceWindowStartDate AND t.AceWindowEndDate THEN t.AceWindowStartDate END AS AceTestingWindowStartDate,
    CASE WHEN s.AssessmentDate BETWEEN t.AceWindowStartDate AND t.AceWindowEndDate THEN t.AceWindowEndDate END AS AceTestingWindowEndDate,
    t.TestingWindow AS StarTestingWindow
  FROM star as s
  CROSS JOIN assessment_ids AS a
  LEFT JOIN testing_windows AS t
  ON s.SchoolYear = t.SchoolYear
  WHERE s.AssessmentDate BETWEEN t.TestingWindowStartDate AND t.TestingWindowEndDate
)

SELECT * FROM final

