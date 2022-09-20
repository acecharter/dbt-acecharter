WITH assessment_ids AS (
  SELECT 
    AceAssessmentId,
    AssessmentNameShort AS AssessmentName
  FROM {{ ref('stg_GSD__Assessments') }}
  WHERE AssessmentNameShort = 'Star Early Literacy'
),

missing_student_ids AS (
  SELECT
    StudentRenaissanceID,
    StudentIdentifier,
    StateUniqueId
  FROM {{ ref('stg_GSD__RenStarMissingStudentIds')}}
),

star_with_missing_ids AS (
  SELECT
    s.* EXCEPT(StudentIdentifier, StudentStateID),
    CASE
      WHEN s.StudentIdentifier IS NULL THEN CAST(m.StudentIdentifier AS INT64)
      ELSE s.StudentIdentifier
    END AS StudentIdentifier,
    CASE
      WHEN s.StudentStateID IS NULL OR s.StudentStateID = 15939 THEN CAST(m.StateUniqueId AS INT64)
      ELSE s.StudentStateID
    END AS StudentStateID,
  FROM {{ source('RawData', 'RenStarEarlyLiteracy2021to2122')}} AS s
  LEFT JOIN missing_student_ids AS m
  USING (StudentRenaissanceID)
),

star_early_lit AS (
  SELECT
    CASE
      WHEN TRIM(SchoolIdentifier)='57b1f93e473b517136000009' THEN '116814'
      WHEN TRIM(SchoolIdentifier)='57b1f93e473b51713600000b' THEN '129247'
      WHEN TRIM(SchoolIdentifier)='57b1f93e473b517136000007' THEN '131656'
      WHEN TRIM(SchoolIdentifier)='061182013023' THEN '125617'
      ELSE '999999999'
    END AS TestedSchoolId,
    SchoolName AS TestedSchoolName,
    CONCAT(LEFT(SchoolYear, 4), '-', RIGHT(SchoolYear, 2)) AS SchoolYear,
    StudentRenaissanceID,
    CAST(StudentIdentifier AS STRING) AS StudentIdentifier,
    CAST(StudentStateID AS STRING) AS StateUniqueId,
    CASE
      WHEN StudentMiddleName IS NULL THEN CONCAT(StudentLastName, ", ", StudentFirstName)
      ELSE CONCAT(StudentLastName, ", ", StudentFirstName, " ", StudentMiddleName)
    END AS DisplayName,
    StudentLastName AS LastName,
    StudentFirstName AS FirstName,
    StudentMiddleName AS MiddleName,
    Gender,
    DATE(BirthDate) AS BirthDate,
    CAST(CurrentGrade AS STRING) As GradeLevel,
    EnrollmentStatus,
    AssessmentID,
    DATE(CompletedDate) AS AssessmentDate,
    AssessmentNumber,
    AssessmentType,
    TotalTimeInSeconds,
    GradePlacement,
    CAST(Grade AS STRING) AS AssessmentGradeLevel,
    CAST(GradeEquivalent AS STRING) AS GradeEquivalent,
    ScaledScore,
    UnifiedScore,
    CAST(NULL AS INT64) AS PercentileRank,
    CAST(NULL AS FLOAT64) AS NormalCurveEquivalent,
    CAST(NULL AS STRING) AS Lexile,
    LiteracyClassification,
    CAST(NULL AS INT64) AS StudentGrowthPercentileFallFall,
    CAST(NULL AS INT64) AS StudentGrowthPercentileFallSpring,
    CAST(NULL AS INT64) AS StudentGrowthPercentileFallWinter,
    CAST(NULL AS INT64) AS StudentGrowthPercentileSpringSpring,
    CAST(NULL AS INT64) AS StudentGrowthPercentileWinterSpring,
    CAST(NULL AS INT64) AS CurrentSGP,
    CAST(NULL AS INT64) AS StateBenchmarkCategoryLevel,
    CAST(NULL AS STRING) AS AceTestingWindowName,
    CAST(NULL AS DATE) AS AceTestingWindowStartDate,
    CAST(NULL AS DATE) AS AceTestingWindowEndDate,
    CASE
      WHEN CompletedDate BETWEEN '2020-08-01' AND '2020-11-30' THEN 'Fall'
      WHEN CompletedDate BETWEEN '2020-12-01' AND '2021-03-31' THEN 'Winter'
      WHEN CompletedDate BETWEEN '2021-04-01' AND'2021-07-31' THEN 'Spring'
      WHEN CompletedDate BETWEEN '2021-08-01' AND '2021-11-30' THEN 'Fall'
      WHEN CompletedDate BETWEEN '2021-12-01' AND '2022-03-31' THEN 'Winter'
      WHEN CompletedDate BETWEEN '2022-04-01' AND'2022-07-31' THEN 'Spring'
    END AS StarTestingWindow
  FROM star_with_missing_ids
),

final AS(
  SELECT
    a.*,
    s.*
  FROM assessment_ids AS a
  CROSS JOIN star_early_lit as s
)

SELECT * FROM final
