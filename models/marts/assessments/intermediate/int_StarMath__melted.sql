WITH star_math AS (
  SELECT * FROM {{ ref('stg_RenaissanceStar__Math_v2') }}
),

star_math_keys AS(
  SELECT
    --ASSESSMENT_KEY,
    --OBJECTIVE_ASSESSMENT_KEY,
    StudentRenaissanceID,
    StudentIdentifier,
    StateUniqueId,
    SchoolId,
    SchoolYear,
    AssessmentID,
    AssessmentDate AS AdministrationDate,
    Grade AS AssessedGradeLevel
  FROM star_math
),

gp AS (
  SELECT
    AssessmentID,
    'Grade Equivalent Minus Grade Placement' AS ReportingMethod,
    'Grade Equivalent points above Grade Placement' AS StudentResultDataType,
    CAST(GradePlacement AS STRING) AS StudentResult
  FROM star_math
),

ge AS (
  SELECT
    AssessmentID,
    'Grade Equivalent' AS ReportingMethod,
    'Grade Equivalent' AS StudentResultDataType,
    CAST(GradeEquivalent AS STRING) AS StudentResult
  FROM star_math
),

unified_score AS (
  SELECT
    AssessmentID,
    'Unified Score' AS ReportingMethod,
    'Unified Score' AS StudentResultDataType,
    CAST(UnifiedScore AS STRING) AS StudentResult
  FROM star_math
), 

percentile_rank AS (
  SELECT
    AssessmentID,
    'Percentile Rank' AS ReportingMethod,
    'Percentile Rank' AS StudentResultDataType,
    CAST(PercentileRank AS STRING) AS StudentResult
  FROM star_math
),

sgp_fall_fall AS (
  SELECT
    AssessmentID,
    'Student Growth Percentile (Fall to Fall)' AS ReportingMethod,
    'Student Growth Percentile' AS StudentResultDataType,
    CAST(StudentGrowthPercentileFallFall AS STRING) AS StudentResult
  FROM star_math
),

sgp_fall_winter AS (
  SELECT
    AssessmentID,
    'Student Growth Percentile (Fall to Winter)' AS ReportingMethod,
    'Student Growth Percentile' AS StudentResultDataType,
    CAST(StudentGrowthPercentileFallWinter AS STRING) AS StudentResult
  FROM star_math
),

sgp_fall_spring AS (
  SELECT
    AssessmentID,
    'Student Growth Percentile (Fall to Spring)' AS ReportingMethod,
    'Student Growth Percentile' AS StudentResultDataType,
    CAST(StudentGrowthPercentileFallSpring AS STRING) AS StudentResult
  FROM star_math
),

sgp_spring_spring AS (
  SELECT
    AssessmentID,
    'Student Growth Percentile (Spring to Spring)' AS ReportingMethod,
    'Student Growth Percentile' AS StudentResultDataType,
    CAST(StudentGrowthPercentileSpringSpring AS STRING) AS StudentResult
  FROM star_math
),

sgp_winter_spring AS (
  SELECT
    AssessmentID,
    'Student Growth Percentile (Winter to Spring)' AS ReportingMethod,
    'Student Growth Percentile' AS StudentResultDataType,
    CAST(StudentGrowthPercentileWinterSpring AS STRING) AS StudentResult
  FROM star_math
),

sgp_current AS (
  SELECT
    AssessmentID,
    'Student Growth Percentile (current)' AS ReportingMethod,
    'Student Growth Percentile' AS StudentResultDataType,
    CAST(CurrentSGP AS STRING) AS StudentResult
  FROM star_math
),

results_unioned AS(
  SELECT * FROM gp
  UNION ALL
  SELECT * FROM ge
  UNION ALL
  SELECT * FROM unified_score
  UNION ALL
  SELECT * FROM percentile_rank
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
FROM star_math_keys AS s
LEFT JOIN results_unioned AS r
USING (AssessmentID)
WHERE StudentResult IS NOT NULL