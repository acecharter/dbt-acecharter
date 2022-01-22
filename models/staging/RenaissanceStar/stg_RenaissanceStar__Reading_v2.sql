With star_reading AS (
  SELECT
    SchoolIdentifier,
    NameofInstitution AS SchoolName,
    SchoolYear,
    DATE(SchoolYearStartDate) AS SchoolYearStartDate,
    DATE(SchoolYearEndDate) AS SchoolYearEndDate,
    StudentRenaissanceID,
    StudentIdentifier,
    StateUniqueId AS SSID,
    LastSurname AS LastName,
    FirstName,
    MiddleName,
    Gender,
    DATE(Birthdate) AS BirthDate,
    Gradelevel AS GradeLevel,
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
    InstructionalReadingLevel,
    Lexile,
    StudentGrowthPercentileFallFall,
    StudentGrowthPercentileFallSpring,
    StudentGrowthPercentileFallWinter,
    StudentGrowthPercentileSpringSpring,
    StudentGrowthPercentileWinterSpring,
    CurrentSGP,
    ScreeningPeriodWindowName,
    DATE(ScreeningWindowStartDate) AS ScreeningWindowStartDate,
    DATE(ScreeningWindowEndDate) AS ScreeningWindowEndDate

FROM {{ source('RenaissanceStar', 'Reading_v2')}}
)

SELECT * FROM star_reading