{{ config(
    materialized='table'
)}}

WITH
  caaspp AS (
    SELECT
      CONCAT(CountyCode, DistrictCode, SchoolCode,'-', TestYear, '-', DemographicId, '-', GradeLevel, '-', TestId) AS AssessmentId,
      *
    FROM {{ ref('int_Caaspp__2_filtered') }}
  ),

  caaspp_keys AS(
    SELECT
      AssessmentId,
      AceAssessmentId,
      AceAssessmentName,
      EntityCode,
      EntityType,
      EntityName,
      EntityNameMid,
      EntityNameShort,
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

  area_1_above AS (
    SELECT
      AssessmentId,
      CASE
        WHEN AceAssessmentId = '1' THEN 'Reading' 
        WHEN AceAssessmentId = '2' THEN 'Concepts & Procedures'
      END AS AssessmentObjective,
      'Percent Above Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(Area1PctAboveStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE Area1PctAboveStandard IS NOT NULL  
  ),

  area_1_near AS (
    SELECT
      AssessmentId,
      CASE
        WHEN AceAssessmentId = '1' THEN 'Reading' 
        WHEN AceAssessmentId = '2' THEN 'Concepts & Procedures'
      END AS AssessmentObjective,
      'Percent Near Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(Area1PctNearStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE Area1PctNearStandard IS NOT NULL  
  ),

  area_1_below AS (
    SELECT
      AssessmentId,
      CASE
        WHEN AceAssessmentId = '1' THEN 'Reading' 
        WHEN AceAssessmentId = '2' THEN 'Concepts & Procedures'
      END AS AssessmentObjective,
      'Percent Below Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(Area1PctBelowStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE Area1PctBelowStandard IS NOT NULL  
  ),

  area_2_above AS (
    SELECT
      AssessmentId,
      CASE
        WHEN AceAssessmentId = '1' THEN 'Writing' 
        WHEN AceAssessmentId = '2' THEN 'Problem Solving and Modeling & Data Analysis'
      END AS AssessmentObjective,
      'Percent Above Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(Area2PctAboveStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE Area2PctAboveStandard IS NOT NULL  
  ),

  area_2_near AS (
    SELECT
      AssessmentId,
      CASE
        WHEN AceAssessmentId = '1' THEN 'Writing' 
        WHEN AceAssessmentId = '2' THEN 'Problem Solving and Modeling & Data Analysis'
      END AS AssessmentObjective,
      'Percent Near Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(Area2PctNearStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE Area2PctNearStandard IS NOT NULL  
  ),

  area_2_below AS (
    SELECT
      AssessmentId,
      CASE
        WHEN AceAssessmentId = '1' THEN 'Writing' 
        WHEN AceAssessmentId = '2' THEN 'Problem Solving and Modeling & Data Analysis'
      END AS AssessmentObjective,
      'Percent Below Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(Area2PctBelowStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE Area2PctBelowStandard IS NOT NULL  
  ),

  area_3_above AS (
    SELECT
      AssessmentId,
      CASE
        WHEN AceAssessmentId = '1' THEN 'Listening' 
        WHEN AceAssessmentId = '2' THEN 'Communicating Reasoning'
      END AS AssessmentObjective,
      'Percent Above Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(Area3PctAboveStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE Area3PctAboveStandard IS NOT NULL  
  ),

  area_3_near AS (
    SELECT
      AssessmentId,
      CASE
        WHEN AceAssessmentId = '1' THEN 'Listening' 
        WHEN AceAssessmentId = '2' THEN 'Communicating Reasoning'
      END AS AssessmentObjective,
      'Percent Near Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(Area3PctNearStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE Area3PctNearStandard IS NOT NULL  
  ),

  area_3_below AS (
    SELECT
      AssessmentId,
      CASE
        WHEN AceAssessmentId = '1' THEN 'Listening' 
        WHEN AceAssessmentId = '2' THEN 'Communicating Reasoning'
      END AS AssessmentObjective,
      'Percent Below Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(Area3PctBelowStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE Area3PctBelowStandard IS NOT NULL  
  ),

  area_4_above AS (
    SELECT
      AssessmentId,
      'Research/Inquiry' AS AssessmentObjective,
      'Percent Above Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(Area4PctAboveStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE
      Area4PctAboveStandard IS NOT NULL
      AND AceAssessmentId = '1'
  ),

  area_4_near AS (
    SELECT
      AssessmentId,
      'Research/Inquiry' AS AssessmentObjective,
      'Percent Near Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(Area4PctNearStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE
      Area4PctNearStandard IS NOT NULL
      AND AceAssessmentId = '1'
  ),

  area_4_below AS (
    SELECT
      AssessmentId,
      'Research/Inquiry' AS AssessmentObjective,
      'Percent Below Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(Area4PctBelowStandard AS STRING) AS SchoolResult
    FROM caaspp
    WHERE
      Area4PctBelowStandard IS NOT NULL
      AND AceAssessmentId = '1'
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
    UNION ALL
    SELECT * FROM area_1_above
    UNION ALL
    SELECT * FROM area_1_near
    UNION ALL
    SELECT * FROM area_1_below
    UNION ALL
    SELECT * FROM area_2_above
    UNION ALL
    SELECT * FROM area_2_near
    UNION ALL
    SELECT * FROM area_2_below
    UNION ALL
    SELECT * FROM area_3_above
    UNION ALL
    SELECT * FROM area_3_near
    UNION ALL
    SELECT * FROM area_3_below
    UNION ALL
    SELECT * FROM area_4_above
    UNION ALL
    SELECT * FROM area_4_near
    UNION ALL
    SELECT * FROM area_4_below
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

