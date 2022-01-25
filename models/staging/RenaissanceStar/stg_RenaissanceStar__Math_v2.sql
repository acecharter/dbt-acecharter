With star_math AS (
  SELECT
    SchoolIdentifier,
    NameofInstitution AS SchoolName,
    SchoolYear,
    DATE(SchoolYearStartDate) AS SchoolYearStartDate,
    DATE(SchoolYearEndDate) AS SchoolYearEndDate,
    StudentRenaissanceID,
    StudentIdentifier,
    StateUniqueId AS SSID,
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
    DATE(CompletedDateLocal) AS CompletedDateLocal,
    CAST(AssessmentNumber AS INT64) AS AssessmentNumber,
    GradePlacement,
    Grade,
    GradeEquivalent,
    CASE WHEN GradeEquivalent='> 12.9' THEN '13.0' ELSE GradeEquivalent END AS GradeEquivalentValue,
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
    Quantile,
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
    
 FROM {{ source('RenaissanceStar', 'Math_v2')}}
)

SELECT * FROM star_math