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
    LastSurname AS LastName,
    FirstName,
    MiddleName,
    Gender,
    DATE(Birthdate) AS Birthdate,
    Gradelevel As GradeLevel,
    EnrollmentStatus,
    AssessmentID,
    DATE(CompletedDateLocal) AS CompletedDateLocal,
    AssessmentNumber,
    GradePlacement,
    Grade,
    UnifiedScore,
    GradeEquivalent,
    CASE WHEN GradeEquivalent='>12.9' THEN '13' ELSE GradeEquivalent END AS GradeEquivalentValue,
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

 FROM landing-zone-acecharter.RenaissanceStar.Math_v2
)

SELECT * FROM star_math