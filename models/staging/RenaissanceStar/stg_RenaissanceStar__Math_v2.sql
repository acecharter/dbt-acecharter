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
    ScreeningPeriodWindowName,
    DATE(ScreeningWindowStartDate) AS ScreeningWindowStartDate,
    DATE(ScreeningWindowEndDate) AS ScreeningWindowEndDate

 FROM {{ source('RenaissanceStar', 'Math_v2')}}
)

SELECT * FROM star_math