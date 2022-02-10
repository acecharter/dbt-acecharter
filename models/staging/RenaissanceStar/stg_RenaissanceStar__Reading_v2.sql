WITH assessment_ids AS (
  SELECT 
    AceAssessmentId,
    AssessmentNameShort AS AssessmentName,
    'Enterprise' AS AssessmentType
  FROM {{ ref('stg_GoogleSheetData__Assessments') }}
  WHERE AssessmentNameShort = 'Star Reading'
),

missing_student_ids AS (
  SELECT
    StudentRenaissanceID,
    StudentIdentifier,
    StateUniqueId
  FROM {{ ref('stg_GoogleSheetData__RenStarMissingStudentIds')}}
),

star_reading_with_missing_ids AS (
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
  FROM {{ source('RenaissanceStar', 'Reading_v2')}} AS s
  LEFT JOIN missing_student_ids AS m
  USING (StudentRenaissanceID)
),


star_reading AS (
  SELECT
    AssessmentType,
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
    GradePlacement,
    Grade,
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
    ScreeningPeriodWindowName AS AceTestingWindowName,
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
    END AS StarTestingWindow,
    CASE
      WHEN
        CompletedDateLocal >= '2021-08-01' AND
        CompletedDateLocal <= '2021-08-10'
      THEN 'Fall (early)'
      WHEN
        CompletedDateLocal >= '2021-08-11' AND
        CompletedDateLocal <= '2021-09-30'
      THEN 'Fall'
      WHEN
        CompletedDateLocal >= '2021-10-01' AND
        CompletedDateLocal <= '2021-11-30'
      THEN 'Fall (late)'
      WHEN
        CompletedDateLocal >= '2021-12-01' AND
        CompletedDateLocal <= '2022-01-14'
      THEN 'Winter'
      WHEN
        CompletedDateLocal >= '2022-01-15' AND
        CompletedDateLocal <= '2022-03-31'
      THEN 'Winter (late)'
      WHEN
        CompletedDateLocal >= '2022-04-01' AND
        CompletedDateLocal <= '2022-04-14'
      THEN 'Spring (early)'
      WHEN
        CompletedDateLocal >= '2022-04-15' AND
        CompletedDateLocal <= '2022-05-31'
      THEN 'Spring'
      WHEN
        CompletedDateLocal >= '2022-06-01' AND
        CompletedDateLocal <= '2022-07-31'
      THEN 'Spring (late)'
    END AS DetailedTestingWindow2122

FROM star_reading_with_missing_ids
)

SELECT
  a.* EXCEPT(AssessmentType),
  s.* EXCEPT(AssessmentType)
FROM star_reading as s
LEFT JOIN assessment_ids AS a
USING (AssessmentType)
