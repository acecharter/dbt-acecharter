{{ config(
    materialized='table'
)}}

WITH
  assessment_ids AS (
    SELECT 
      AceAssessmentId,
      AssessmentNameShort AS AceAssessmentName,
      AssessmentSubject
    FROM {{ ref('stg_GSD__Assessments') }}
    WHERE AssessmentNameShort = 'CAST'
  ),
  
  entities AS (
    SELECT * FROM {{ ref('dim_Entities')}}
  ),
  
  min_met_scores AS (
    SELECT
      AceAssessmentId,
      GradeLevel,
      MinStandardMetScaleScore
    FROM {{ ref('fct_CaasppMinMetScaleScores') }}
    WHERE Area='Overall'
  ),

  cast_unioned AS (
    SELECT * FROM {{ ref('base_RD__Cast2019')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Cast2021')}}
  ),

  cast_entity_codes_added AS (
    SELECT
      *,
      CASE
        WHEN DistrictCode = '00000' THEN CountyCode
        WHEN SchoolCode = '0000000' THEN DistrictCode
        ELSE SchoolCode
      END AS EntityCode
    FROM cast_unioned
  ),

  cast_filtered AS (
    SELECT
      a.AceAssessmentId,
      a.AceAssessmentName,
      e.*,
      c.* EXCEPT(EntityCode, Filler)
    FROM cast_entity_codes_added AS c
    CROSS JOIN assessment_ids AS a
    LEFT JOIN entities as e
    ON c.EntityCode = e.EntityCode
    WHERE c.EntityCode IN (SELECT EntityCode FROM entities)
  ),

  cast_sy_and_dfs_added AS (
    SELECT
      c.*,
      CONCAT( CAST(c.TestYear - 1 AS STRING), '-', CAST(c.TestYear - 2000 AS STRING) ) AS SchoolYear,
      CAST(
        CASE WHEN c.MeanScaleScore IS NOT NULL THEN ROUND(c.MeanScaleScore - m.MinStandardMetScaleScore, 1) ELSE NULL END AS STRING
      ) AS MeanDistanceFromStandard
    FROM cast_filtered AS c
    LEFT JOIN min_met_scores AS m
    ON
      c.AceAssessmentId = m.AceAssessmentId
      AND CAST(c.GradeLevel AS STRING) = m.GradeLevel
  ),

  final AS (
    SELECT
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
      SchoolYear,
      TestYear,
      DemographicId,
      TestType,
      TotalTestedAtReportingLevel
      TotalTestedWithScoresAtReportingLevel,
      GradeLevel,
      TestId,
      StudentsEnrolled,
      StudentsTested,
      MeanScaleScore,
      PctStandardExceeded,
      PctStandardMet,
      PctStandardMetAndAbove,
      PctStandardNearlyMet,
      PctStandardNotMet,
      StudentsWithScores,
      LifeSciencesDomainPercentBelowStandard,
      LifeSciencesDomainPercentNearStandard,
      LifeSciencesDomainPercentAboveStandard,
      PhysicalSciencesDomainPercentBelowStandard,
      PhysicalSciencesDomainPercentNearStandard,
      PhysicalSciencesDomainPercentAboveStandard,
      EarthAndSpaceSciencesDomainPercentBelowStandard,
      EarthAndSpaceSciencesDomainPercentNearStandard,
      EarthAndSpaceSciencesDomainPercentAboveStandard,
      TypeId,
      MeanDistanceFromStandard
    FROM cast_sy_and_dfs_added
  )

SELECT * FROM final