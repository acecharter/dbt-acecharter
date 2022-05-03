{{ config(
    materialized='table'
)}}

WITH
  elpac AS (
    SELECT
      CONCAT(CountyCode, DistrictCode, SchoolCode,'-', TestYear, '-', StudentGroupId, '-', GradeLevel, '-', AssessmentType) AS AssessmentId,
      *
    FROM {{ ref('int_Elpac__unioned_filtered') }}
  ),

  elpac_keys AS(
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
      RecordType,
      StudentGroupId,
      GradeLevel,
      AssessmentType,
      TotalEnrolled,
      CASE WHEN TestYear = 2018 THEN TotalTested ELSE TotalTestedWithScores END AS TotalTestedWithScores
    FROM elpac
  ),

  mean_scale_score AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Mean Scale Score' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(OverallMeanSclScr AS STRING) AS SchoolResult,
      CAST(NULL AS INT64) AS StudentWithResultCount
    FROM elpac
    WHERE OverallMeanSclScr IS NOT NULL
  ),

  pct_level1 AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Level 1' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(OverallPerfLvl1Pcnt AS STRING) AS SchoolResult,
      OverallPerfLvl1Count AS StudentWithResultCount
    FROM elpac
    WHERE OverallPerfLvl1Pcnt IS NOT NULL
  ),

  pct_level2 AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Level 2' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(OverallPerfLvl2Pcnt AS STRING) AS SchoolResult,
      OverallPerfLvl2Count AS StudentWithResultCount
    FROM elpac
    WHERE OverallPerfLvl2Pcnt IS NOT NULL
  ),

  pct_level3 AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Level 3' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(OverallPerfLvl3Pcnt AS STRING) AS SchoolResult,
      OverallPerfLvl3Count AS StudentWithResultCount
    FROM elpac
    WHERE OverallPerfLvl3Pcnt IS NOT NULL
  ),

  pct_level4 AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Level 4' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(OverallPerfLvl4Pcnt AS STRING) AS SchoolResult,
      OverallPerfLvl4Count AS StudentWithResultCount
    FROM elpac
    WHERE OverallPerfLvl4Pcnt IS NOT NULL
  ),

  results_unioned AS(
    SELECT * FROM mean_scale_score
    UNION ALL
    SELECT * FROM pct_level1
    UNION ALL
    SELECT * FROM pct_level2
    UNION ALL
    SELECT * FROM pct_level3
    UNION ALL
    SELECT * FROM pct_level4
  ),

  final AS (
    SELECT
      k.*,
      r.* EXCEPT (AssessmentID)
    FROM elpac_keys AS k
    LEFT JOIN results_unioned AS r
    USING (AssessmentId)
  )


SELECT * FROM final

