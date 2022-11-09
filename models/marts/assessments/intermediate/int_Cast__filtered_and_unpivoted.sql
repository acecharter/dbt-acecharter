{{ config(
    materialized='table'
)}}

WITH
  cast_results AS (
    SELECT
      CONCAT(CountyCode, DistrictCode, SchoolCode,'-', TestYear, '-', DemographicId, '-', GradeLevel, '-', TestId) AS AssessmentId,
      *
    FROM {{ ref('stg_RD__Cast')}}
    WHERE
      GradeLevel >= 5
      AND DemographicId IN (
        '1',   --All Students
        '128', --Reported Disabilities
        '31',  --Economic disadvantaged
        '160', --EL (English learner)
        '78',  --Hispanic or Latino
        '204'  --Economically disadvantaged Hispanic or Latino
        )
  ),

  cast_keys AS(
    SELECT
      AssessmentId,
      AceAssessmentId,
      AceAssessmentName,
      AssessmentSubject,
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
    FROM cast_results
  ),

  mean_scale_score AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Mean Scale Score' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(MeanScaleScore AS STRING) AS SchoolResult
    FROM cast_results
    WHERE MeanScaleScore IS NOT NULL
  ),

  mean_dfs AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Mean Distance From Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(MeanDistanceFromStandard AS STRING) AS SchoolResult
    FROM cast_results
    WHERE MeanDistanceFromStandard IS NOT NULL
  ),

  pct_met_and_above AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Met and Above' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PctStandardMetAndAbove AS STRING) AS SchoolResult
    FROM cast_results
    WHERE PctStandardMetAndAbove IS NOT NULL
  ),

  pct_exceeded AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Exceeded' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PctStandardExceeded AS STRING) AS SchoolResult
    FROM cast_results
    WHERE PctStandardExceeded IS NOT NULL
  ),

  pct_met AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Met' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PctStandardMet AS STRING) AS SchoolResult
    FROM cast_results
    WHERE PctStandardMet IS NOT NULL
  ),


  pct_nearly_met AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Nearly Met' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PctStandardNearlyMet AS STRING) AS SchoolResult
    FROM cast_results
    WHERE PctStandardNearlyMet IS NOT NULL
  ),

  pct_not_met AS (
    SELECT
      AssessmentId,
      'Overall' AS AssessmentObjective,
      'Percent Not Met' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PctStandardNotMet AS STRING) AS SchoolResult
    FROM cast_results
    WHERE PctStandardNotMet IS NOT NULL
  ),

  life_sciences_above AS (
    SELECT
      AssessmentId,
      'Life Sciences Domain'AS AssessmentObjective,
      'Percent Above Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(LifeSciencesDomainPercentAboveStandard AS STRING) AS SchoolResult
    FROM cast_results
    WHERE LifeSciencesDomainPercentAboveStandard IS NOT NULL  
  ),

  life_sciences_near AS (
    SELECT
      AssessmentId,
      'Life Sciences Domain'AS AssessmentObjective,
      'Percent Near Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(LifeSciencesDomainPercentNearStandard AS STRING) AS SchoolResult
    FROM cast_results
    WHERE LifeSciencesDomainPercentNearStandard IS NOT NULL  
  ),

  life_sciences_below AS (
    SELECT
      AssessmentId,
      'Life Sciences Domain'AS AssessmentObjective,
      'Percent Below Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(LifeSciencesDomainPercentBelowStandard AS STRING) AS SchoolResult
    FROM cast_results
    WHERE LifeSciencesDomainPercentBelowStandard IS NOT NULL  
  ),

  physical_sciences_above AS (
    SELECT
      AssessmentId,
      'Physical Sciences Domain'AS AssessmentObjective,
      'Percent Above Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PhysicalSciencesDomainPercentAboveStandard AS STRING) AS SchoolResult
    FROM cast_results
    WHERE PhysicalSciencesDomainPercentAboveStandard IS NOT NULL  
  ),

  physical_sciences_near AS (
    SELECT
      AssessmentId,
      'Physical Sciences Domain'AS AssessmentObjective,
      'Percent Near Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PhysicalSciencesDomainPercentNearStandard AS STRING) AS SchoolResult
    FROM cast_results
    WHERE PhysicalSciencesDomainPercentNearStandard IS NOT NULL  
  ),

  physical_sciences_below AS (
    SELECT
      AssessmentId,
      'Physical Sciences Domain'AS AssessmentObjective,
      'Percent Below Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(PhysicalSciencesDomainPercentBelowStandard AS STRING) AS SchoolResult
    FROM cast_results
    WHERE PhysicalSciencesDomainPercentBelowStandard IS NOT NULL  
  ),

  earth_and_space_sciences_above AS (
    SELECT
      AssessmentId,
      'Earth and Space Sciences Domain'AS AssessmentObjective,
      'Percent Above Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(EarthAndSpaceSciencesDomainPercentAboveStandard AS STRING) AS SchoolResult
    FROM cast_results
    WHERE EarthAndSpaceSciencesDomainPercentAboveStandard IS NOT NULL  
  ),

  earth_and_space_sciences_near AS (
    SELECT
      AssessmentId,
      'Earth and Space Sciences Domain'AS AssessmentObjective,
      'Percent Near Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(EarthAndSpaceSciencesDomainPercentNearStandard AS STRING) AS SchoolResult
    FROM cast_results
    WHERE EarthAndSpaceSciencesDomainPercentNearStandard IS NOT NULL  
  ),

  earth_and_space_sciences_below AS (
    SELECT
      AssessmentId,
      'Earth and Space Sciences Domain'AS AssessmentObjective,
      'Percent Below Standard' AS ReportingMethod,
      'FLOAT64' AS ResultDataType,
      CAST(EarthAndSpaceSciencesDomainPercentBelowStandard AS STRING) AS SchoolResult
    FROM cast_results
    WHERE EarthAndSpaceSciencesDomainPercentBelowStandard IS NOT NULL  
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
    SELECT * FROM life_sciences_above
    UNION ALL
    SELECT * FROM life_sciences_near
    UNION ALL
    SELECT * FROM life_sciences_below
    UNION ALL
    SELECT * FROM physical_sciences_above
    UNION ALL
    SELECT * FROM physical_sciences_near
    UNION ALL
    SELECT * FROM physical_sciences_below
    UNION ALL
    SELECT * FROM earth_and_space_sciences_above
    UNION ALL
    SELECT * FROM earth_and_space_sciences_near
    UNION ALL
    SELECT * FROM earth_and_space_sciences_below
  ),

  final AS (
    SELECT
      k.*,
      r.* EXCEPT (AssessmentID),
      CASE
        WHEN r.ReportingMethod LIKE 'Mean%' THEN StudentsWithScores 
        ELSE ROUND(StudentsWithScores * CAST(SchoolResult AS FLOAT64), 0)
      END AS StudentWithResultCount
    FROM cast_keys AS k
    LEFT JOIN results_unioned AS r
    USING (AssessmentId)
  )


SELECT * FROM final
