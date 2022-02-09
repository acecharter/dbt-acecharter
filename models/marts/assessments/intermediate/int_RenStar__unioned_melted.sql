WITH star_results AS (
  SELECT * FROM {{ ref('int_RenStar__unioned') }}
),

star_keys AS(
  SELECT
    AceAssessmentId,
    StudentRenaissanceID,
    StudentIdentifier,
    StateUniqueId,
    TestedSchoolId,
    SchoolYear,
    AssessmentId,
    AssessmentDate AS AdministrationDate,
    Grade AS AssessedGradeLevel,
    GradePlacement
  FROM star_results
),

ge AS (
  SELECT
    AssessmentID,
    'Grade Equivalent' AS ReportingMethod,
    'STRING' AS StudentResultDataType,
    CAST(GradeEquivalent AS STRING) AS StudentResult
  FROM star_results
),

unified_score AS (
  SELECT
    AssessmentID,
    'Unified Score' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(UnifiedScore AS STRING) AS StudentResult
  FROM star_results
), 

percentile_rank AS (
  SELECT
    AssessmentID,
    'Percentile Rank' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(PercentileRank AS STRING) AS StudentResult
  FROM star_results
),

lexile AS (
  SELECT
    AssessmentID,
    'Lexile Level' AS ReportingMethod,
    'STRING' AS StudentResultDataType,
    CAST(Lexile AS STRING) AS StudentResult
  FROM star_results
),

sgp_fall_fall AS (
  SELECT
    AssessmentID,
    'SGP (Fall to Fall)' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(StudentGrowthPercentileFallFall AS STRING) AS StudentResult
  FROM star_results
),

sgp_fall_winter AS (
  SELECT
    AssessmentID,
    'SGP (Fall to Winter)' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(StudentGrowthPercentileFallWinter AS STRING) AS StudentResult
  FROM star_results
),

sgp_fall_spring AS (
  SELECT
    AssessmentID,
    'SGP (Fall to Spring)' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(StudentGrowthPercentileFallSpring AS STRING) AS StudentResult
  FROM star_results
),

sgp_spring_spring AS (
  SELECT
    AssessmentID,
    'SGP (Spring to Spring)' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(StudentGrowthPercentileSpringSpring AS STRING) AS StudentResult
  FROM star_results
),

sgp_winter_spring AS (
  SELECT
    AssessmentID,
    'SGP (Winter to Spring)' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(StudentGrowthPercentileWinterSpring AS STRING) AS StudentResult
  FROM star_results
),

sgp_current AS (
  SELECT
    AssessmentID,
    'SGP (current)' AS ReportingMethod,
    'INT64' AS StudentResultDataType,
    CAST(CurrentSGP AS STRING) AS StudentResult
  FROM star_results
),

results_unioned AS(
  SELECT * FROM ge
  UNION ALL
  SELECT * FROM unified_score
  UNION ALL
  SELECT * FROM percentile_rank
  UNION ALL
  SELECT * FROM lexile
  UNION ALL
  SELECT * FROM sgp_fall_fall
  UNION ALL
  SELECT * FROM sgp_fall_winter
  UNION ALL
  SELECT * FROM sgp_fall_spring
  UNION ALL
  SELECT * FROM sgp_spring_spring
  UNION ALL
  SELECT * FROM sgp_winter_spring
  UNION ALL
  SELECT * FROM sgp_current

)

SELECT
 s.*,
 r.* EXCEPT (AssessmentID)
FROM star_keys AS s
LEFT JOIN results_unioned AS r
USING (AssessmentID)
WHERE StudentResult IS NOT NULL