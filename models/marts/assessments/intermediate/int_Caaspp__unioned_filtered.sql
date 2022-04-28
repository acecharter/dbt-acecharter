{{ config(
    materialized='table'
)}}

WITH 
  comparison_entities AS (
    SELECT DISTINCT * EXCEPT(AceComparisonSchoolCode, AceComparisonSchoolName)
    FROM {{ ref('dim_ComparisonEntities')}}
  ),

  caaspp AS (
    SELECT *
    FROM {{ ref('int_Caaspp__unioned')}}
    WHERE
      GradeLevel >= 5
      AND EntityCode IN (SELECT EntityCode FROM comparison_entities)
      AND DemographicId IN (
        '1',   --All Students
        '128', --Reported Disabilities
        '31',  --Economic disadvantaged
        '160', --EL (English learner)
        '78',  --Hispanic or Latino
        '204'  --Economically disadvantaged Hispanic or Latino
      )
  ),
  
  min_met_scores AS (
    SELECT
      AceAssessmentId,
      GradeLevel,
      MinStandardMetScaleScore
    FROM {{ ref('fct_CaasppMinMetScaleScores') }}
    WHERE Area='Overall'
  ),

  final AS (
    SELECT
      c.*,
      e.* EXCEPT (EntityCode),
      CONCAT(
        CAST(TestYear - 1 AS STRING), '-', CAST(TestYear - 2000 AS STRING)
      ) AS SchoolYear,
      CAST(
        CASE WHEN c.MeanScaleScore IS NOT NULL THEN ROUND(c.MeanScaleScore - m.MinStandardMetScaleScore, 1) ELSE NULL END AS STRING
      ) AS MeanDistanceFromStandard
    FROM caaspp AS c
    LEFT JOIN comparison_entities AS e
    ON c.EntityCode = e.EntityCode
    LEFT JOIN min_met_scores AS m
    ON
      c.AceAssessmentId = m.AceAssessmentId
      AND CAST(c.GradeLevel AS STRING) = m.GradeLevel
  )

SELECT * FROM final
