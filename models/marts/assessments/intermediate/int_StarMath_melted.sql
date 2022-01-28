WITH star_reading AS (
  SELECT * 
    ASSESSMENT_KEY,
    OBJECTIVE_ASSESSMENT_KEY,
    StudentRenaissanceID,
    StudentIdentifier,
    SSID,
    SchoolId,
    SchoolYear,
    AssessmentID,
    AssessmentDate AS AdministrationDate,
    Grade AS AssessedGradeLevel
  
),

ge_minus_gp AS (
  SELECT
    AssessmentID,
    'Grade Equivalent Minus Grade Placement' AS ReportingMethod,
    'Grade Equivalent points above Grade Placement' AS StudentResultDataType
    GradeEquivalent - GradePlacement AS StudentResult
)

ge AS (
    AssessmentID,
    'Grade Equivalent' AS ReportingMethod,
    'Grade Equivalent' AS StudentResultDataType
    GradeEquivalent AS StudentResult
),

unified_score AS (
    AssessmentID,
    'Unified Score' AS ReportingMethod,
    'Unified Score' AS StudentResultDataType
    UnifiedScore AS StudentResult
), 

percentile_rank AS (
    AssessmentID,
    'Percentile Rank' AS ReportingMethod,
    'Percentile Rank' AS StudentResultDataType
    PercentileRank AS StudentResult
),

lexile AS (
    AssessmentID,
    'Lexile Level' AS ReportingMethod,
    'Lexile Level' AS StudentResultDataType
    Lexile AS StudentResult
),

sgp_fall_fall AS (
    AssessmentID,
    'Student Growth Percentile (Fall to Fall)' AS ReportingMethod,
    'Student Growth Percentile' AS StudentResultDataType
    StudentGrowthPercentileFallFall AS StudentResult
),

sgp_fall_winter AS (
    AssessmentID,
    'Student Growth Percentile (Fall to Winter)' AS ReportingMethod,
    'Student Growth Percentile' AS StudentResultDataType
    StudentGrowthPercentileFallWinter AS StudentResult
),

sgp_fall_spring AS (
    AssessmentID,
    'Student Growth Percentile (Fall to Spring)' AS ReportingMethod,
    'Student Growth Percentile' AS StudentResultDataType
    StudentGrowthPercentileFallSpring AS StudentResult
),

sgp_spring_spring AS (
    AssessmentID,
    'Student Growth Percentile (Spring to Spring)' AS ReportingMethod,
    'Student Growth Percentile' AS StudentResultDataType
    StudentGrowthPercentileSpringSpring AS StudentResult
),

sgp_winter_spring AS (
    AssessmentID,
    'Student Growth Percentile (Winter to Spring)' AS ReportingMethod,
    'Student Growth Percentile' AS StudentResultDataType
    StudentGrowthPercentileWinterSpring AS StudentResult
),

sgp_current AS (
    AssessmentID,
    'Student Growth Percentile (current)' AS ReportingMethod,
    'Student Growth Percentile' AS StudentResultDataType
    CurrentSGP AS StudentResult
),

results_unioned AS(
  SELECT * FROM ge_minus_gp
  UNION ALL
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
 r.*
FROM star_reading AS s
LEFT JOIN results_unioned AS r
USING (AssessmentID)