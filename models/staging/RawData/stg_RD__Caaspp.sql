WITH
  assessment_ids AS (
    SELECT 
      AceAssessmentId,
      AssessmentNameShort AS AceAssessmentName,
      SystemOrVendorAssessmentId
    FROM {{ ref('stg_GSD__Assessments') }}
    WHERE SystemOrVendorName = 'CAASPP'
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

  caaspp_unioned AS (
    SELECT * FROM {{ ref('base_RD__Caaspp2015')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Caaspp2016')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Caaspp2017')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Caaspp2018')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Caaspp2019')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Caaspp2021')}}
  ),

  caaspp_entity_codes_added AS (
    SELECT
      *,
      CASE
        WHEN DistrictCode = '00000' THEN CountyCode
        WHEN SchoolCode = '0000000' THEN DistrictCode
        ELSE SchoolCode
      END AS EntityCode
    FROM caaspp_unioned
  ),

  caaspp_filtered AS (
    SELECT
      a.AceAssessmentId,
      a.AceAssessmentName,
      e.*,
      c.* EXCEPT(EntityCode, Filler)
    FROM caaspp_entity_codes_added AS c
    LEFT JOIN assessment_ids AS a
    ON c.TestId = a.SystemOrVendorAssessmentId
    LEFT JOIN entities as e
    ON c.EntityCode = e.EntityCode
    WHERE c.EntityCode IN (SELECT EntityCode FROM entities)
  ),

  caaspp_sy_and_dfs_added AS (
    SELECT
      c.*,
      CONCAT( CAST(c.TestYear - 1 AS STRING), '-', CAST(c.TestYear - 2000 AS STRING) ) AS SchoolYear,
      CAST(
        CASE WHEN c.MeanScaleScore IS NOT NULL THEN ROUND(c.MeanScaleScore - m.MinStandardMetScaleScore, 1) ELSE NULL END AS STRING
      ) AS MeanDistanceFromStandard
    FROM caaspp_filtered AS c
    LEFT JOIN min_met_scores AS m
    ON
      c.AceAssessmentId = m.AceAssessmentId
      AND CAST(c.GradeLevel AS STRING) = m.GradeLevel
  ),

  final AS (
    SELECT
      AceAssessmentId,
      AceAssessmentName,
      EntityCode,
      EntityType,
      EntityName,
      EntityNameMid,
      EntityNameShort,
      SchoolYear,
      CountyCode,
      DistrictCode,
      SchoolCode,
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
      Area1PctAboveStandard,
      Area1PctNearStandard,
      Area1PctBelowStandard,
      Area2PctAboveStandard,
      Area2PctNearStandard,
      Area2PctBelowStandard,
      Area3PctAboveStandard,
      Area3PctNearStandard,
      Area3PctBelowStandard,
      Area4PctAboveStandard,
      Area4PctNearStandard,
      Area4PctBelowStandard,
      TypeId
    FROM caaspp_sy_and_dfs_added
  )

SELECT * FROM final