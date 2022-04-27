{{ config(
    materialized='table'
)}}

WITH
  caaspp AS (
    SELECT
      CONCAT(CountyCode, DistrictCode, SchoolCode,'-', TestYear, '-', DemographicId, '-', GradeLevel, '-', TestId) AS AssessmentId,
      *
    FROM {{ ref('int_Caaspp__unioned_filtered') }}
  ),

  caaspp_keys AS(
    SELECT
      AssessmentId,
      AceAssessmentId,
      AceAssessmentName,
      EntityCode,
      EntityType,
      CountyCode,
      DistrictCode,
      SchoolCode,
      TestYear,
      SchoolYear,
      TypeId,
      DemographicId,
      GradeLevel,
      TestId,
      StudentsEnrolled,
      StudentsWithScores
    FROM caaspp
  ),

  mean_scale_score AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Mean Scale Score' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(MeanScaleScore AS STRING) AS SchoolResult
    FROM caaspp
    WHERE MeanScaleScore IS NOT NULL
  ),

  mean_dfs AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Mean Distrance From Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(MeanDistanceFromStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE MeanDistanceFromStandard IS NOT NULL
  ),

  pct_met_and_above AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Met and Above' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PctStandardMetAndAbove AS STRING) AS SchoolResult
    FROM caaspp
    WHERE PctStandardMetAndAbove IS NOT NULL
  ),

  pct_exceeded AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Exceeded' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PctStandardExceeded AS STRING) AS SchoolResult
    FROM caaspp
    WHERE PctStandardExceeded IS NOT NULL
  ),

  pct_met AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Met' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PctStandardMet AS STRING) AS SchoolResult
    FROM caaspp
    WHERE PctStandardMet IS NOT NULL
  ),


  pct_nearly_met AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Nearly Met' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PctStandardNearlyMet AS STRING) AS SchoolResult
    FROM caaspp
    WHERE PctStandardNearlyMet IS NOT NULL
  ),

  pct_not_met AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Not Met' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PctStandardNotMet AS STRING) AS SchoolResult
    FROM caaspp
    WHERE PctStandardNotMet IS NOT NULL
  ),

  results_unioned AS(
    SELECT * FROM mean_scale_score
    UNION ALL
    SELECT * FROM mean_dfs
    UNION ALL
    SELECT * FROM pct_met_and_above
    UNION ALL
    SELECT * FROM pct_exceeded
    UNION ALL
    SELECT * FROM pct_met
    UNION ALL
    SELECT * FROM pct_nearly_met
    UNION ALL
    SELECT * FROM pct_not_met
  ),

  final AS (
    SELECT
      k.*,
      r.* EXCEPT (AssessmentID),
      CASE
        WHEN r.ReportingMethod LIKE 'Mean%' THEN NULL 
        ELSE ROUND(StudentsWithScores * CAST(SchoolResult AS FLOAT64), 0)
      END AS StudentWithResultCount
    FROM caaspp_keys AS k
    LEFT JOIN results_unioned AS r
    USING (AssessmentId)
  )


SELECT * FROM final

