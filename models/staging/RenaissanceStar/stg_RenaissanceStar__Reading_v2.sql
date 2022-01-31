WITH assessment_ids AS (
  SELECT 
    AceAssessmentId,
    AssessmentNameShort AS AssessmentName,
    'Enterprise' AS AssessmentType
  FROM {{ ref('stg_GoogleSheetData__Assessments') }}
  WHERE AssessmentNameShort = 'Star Reading'
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
    END AS SchoolId,
    NameofInstitution AS SchoolName,
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
    END AS StarTestingWindow
FROM {{ source('RenaissanceStar', 'Reading_v2')}}
)

SELECT
  a.* EXCEPT(AssessmentType),
  s.* EXCEPT(AssessmentType)
FROM star_reading as s
LEFT JOIN assessment_ids AS a
USING (AssessmentType)
