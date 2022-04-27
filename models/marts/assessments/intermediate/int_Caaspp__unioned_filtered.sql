{{ config(
    materialized='table'
)}}

WITH 
  ace_schools AS (
    SELECT * FROM {{ ref('stg_GSD__Schools')}}
  ),
  
  comparison_entities AS (
    SELECT * FROM {{ ref('stg_GSD__ComparisonEntities')}} 
  ),

  caaspp AS (
    SELECT *
    FROM {{ ref('int_Caaspp__unioned')}}
    WHERE
      GradeLevel >= 5
      AND (
        EntityCode IN (SELECT StateSchoolCode FROM ace_schools)
        OR EntityCode IN (SELECT EntityCode FROM comparison_entities)    
      )
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
      CAST(
        CASE
          WHEN c.MeanScaleScore IS NOT NULL THEN ROUND(c.MeanScaleScore - m.MinStandardMetScaleScore, 1)
          ELSE NULL
        END AS STRING
      ) AS MeanDistanceFromStandard
    FROM caaspp AS c
    LEFT JOIN min_met_scores AS m
    ON
      c.AceAssessmentId = m.AceAssessmentId
      AND CAST(c.GradeLevel AS STRING) = m.GradeLevel
  )

SELECT * FROM final
