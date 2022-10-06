WITH
testing_windows AS (
  SELECT *
  FROM {{ ref('stg_GSD__RenStarTestingWindows') }}
),

missing_student_ids AS (
  SELECT
    StudentRenaissanceID,
    StudentIdentifier,
    StateUniqueId
  FROM {{ ref('base_RSA__MissingStudentIds')}}
),

star_with_missing_ids AS (
  SELECT
    s.* EXCEPT(StudentIdentifier, StateUniqueId),
    CASE
      WHEN s.StudentIdentifier IS NULL THEN m.StudentIdentifier
      ELSE s.StudentIdentifier
    END AS StudentIdentifier,
    CASE
      WHEN s.StateUniqueId IS NULL THEN m.StateUniqueId
      ELSE s.StateUniqueId
    END AS StateUniqueId,
  FROM {{ source('RenaissanceStar_Archive', 'Reading_v2_SY22')}} AS s
  LEFT JOIN missing_student_ids AS m
  USING (StudentRenaissanceID)
),


star AS (
  SELECT
    CASE
      WHEN AssessmentType = 'Enterprise' THEN '11'
      WHEN AssessmentType = 'ProgressMonitoring' THEN '24'
    END AS AceAssessmentId,
    CASE
      WHEN AssessmentType = 'Enterprise' THEN 'Star Reading'
      WHEN AssessmentType = 'ProgressMonitoring' THEN 'Star Reading Progress Monitoring'
    END AS AssessmentName,
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
    InstructionalReadingLevel,
    Lexile,
    StudentGrowthPercentileFallFall,
    StudentGrowthPercentileFallSpring,
    StudentGrowthPercentileFallWinter,
    StudentGrowthPercentileSpringSpring,
    StudentGrowthPercentileWinterSpring,
    CurrentSGP,
    CAST(RIGHT(StateBenchmarkCategoryName, 1) AS INT64) AS StateBenchmarkCategoryLevel
FROM star_with_missing_ids
),

final AS (
  SELECT
    s.*,
    CASE WHEN s.AssessmentDate BETWEEN t.AceWindowStartDate AND t.AceWindowEndDate THEN t.TestingWindow END AS AceTestingWindowName,
    CASE WHEN s.AssessmentDate BETWEEN t.AceWindowStartDate AND t.AceWindowEndDate THEN t.AceWindowStartDate END AS AceTestingWindowStartDate,
    CASE WHEN s.AssessmentDate BETWEEN t.AceWindowStartDate AND t.AceWindowEndDate THEN t.AceWindowEndDate END AS AceTestingWindowEndDate,
    t.TestingWindow AS StarTestingWindow
  FROM star as s
  LEFT JOIN testing_windows AS t
  ON s.SchoolYear = t.SchoolYear
  WHERE s.AssessmentDate BETWEEN t.TestingWindowStartDate AND t.TestingWindowEndDate
)

SELECT * FROM final
