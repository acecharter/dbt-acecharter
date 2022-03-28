WITH assessment_ids AS (
  SELECT 
    AceAssessmentId,
    AssessmentNameShort AS AssessmentName
  FROM {{ ref('stg_GSD__Assessments') }}
  WHERE AssessmentNameShort = 'Star Math'
),

missing_student_ids AS (
  SELECT
    StudentRenaissanceID,
    StudentIdentifier,
    StateUniqueId
  FROM {{ ref('stg_GSD__RenStarMissingStudentIds')}}
),

star_math_with_missing_ids AS (
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
  FROM {{ source('RenaissanceStar', 'Math_v2')}} AS s
  LEFT JOIN missing_student_ids AS m
  USING (StudentRenaissanceID)
),

star_math AS (
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
    Gradelevel As GradeLevel,
    EnrollmentStatus,
    AssessmentID,
    DATE(CompletedDateLocal) AS AssessmentDate,
    CAST(AssessmentNumber AS INT64) AS AssessmentNumber,
    AssessmentType,
    GradePlacement,
    Grade,
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
    CAST(RIGHT(StateBenchmarkCategoryName, 1) AS INT64) AS StateBenchmarkCategoryLevel,
    Quantile,
    CASE
      WHEN
        CompletedDateLocal >= DATE(CONCAT(EXTRACT(YEAR FROM SchoolYearStartDate), '-08-11')) AND
        CompletedDateLocal <= DATE(CONCAT(EXTRACT(YEAR FROM SchoolYearStartDate), '-09-30'))
      THEN 'Fall'
      WHEN
        CompletedDateLocal >= DATE(CONCAT(EXTRACT(YEAR FROM SchoolYearStartDate), '-12-01')) AND
        CompletedDateLocal <= DATE(CONCAT(EXTRACT(YEAR FROM SchoolYearEndDate), '-01-24'))
      THEN 'Winter'
      WHEN
        CompletedDateLocal >= DATE(CONCAT(EXTRACT(YEAR FROM SchoolYearEndDate), '-04-15')) AND
        CompletedDateLocal <= DATE(CONCAT(EXTRACT(YEAR FROM SchoolYearEndDate), '-05-31'))
      THEN 'Spring'
    END AS AceTestingWindowName,
    DATE(ScreeningWindowStartDate) AS AceTestingWindowStartDate,
    DATE(ScreeningWindowEndDate) AS AceTestingWindowEndDate,
    CASE
      WHEN
        CompletedDateLocal >= DATE(CONCAT(EXTRACT(YEAR FROM SchoolYearStartDate), '-08-01')) AND
        CompletedDateLocal <= DATE(CONCAT(EXTRACT(YEAR FROM SchoolYearStartDate), '-11-30'))
      THEN 'Fall'
      WHEN
        CompletedDateLocal >= DATE(CONCAT(EXTRACT(YEAR FROM SchoolYearStartDate), '-12-01')) AND
        CompletedDateLocal <= DATE(CONCAT(EXTRACT(YEAR FROM SchoolYearEndDate), '-03-31'))
      THEN 'Winter'
      WHEN
        CompletedDateLocal >= DATE(CONCAT(EXTRACT(YEAR FROM SchoolYearEndDate), '-04-01')) AND
        CompletedDateLocal <= DATE(CONCAT(EXTRACT(YEAR FROM SchoolYearEndDate), '-07-31'))
      THEN 'Spring'
    END AS StarTestingWindow
    
 FROM star_math_with_missing_ids
)

SELECT
  a.*,
  s.*
FROM star_math as s
CROSS JOIN assessment_ids AS a

