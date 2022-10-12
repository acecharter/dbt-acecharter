{{ config(
    materialized='table'
)}}

WITH

  cers_1819 AS(
    SELECT * FROM {{ ref('stg_RD__Cers1819') }}
  ),

  cers_1920 AS(
    SELECT * FROM {{ ref('stg_RD__Cers1920') }}
  ),

  cers_2021 AS(
    SELECT * FROM {{ ref('stg_RD__Cers2021') }}
  ),

  cers_2122 AS(
    SELECT * FROM {{ ref('stg_RD__Cers2122') }}
  ),

  cers_2223 AS(
    SELECT * FROM {{ ref('stg_RD__Cers2223') }}
  ),

  unioned AS (
    SELECT * FROM cers_1819 
    UNION ALL
    SELECT * FROM cers_1920
    UNION ALL
    SELECT * FROM cers_2021
    UNION ALL
    SELECT * FROM cers_2122
    UNION ALL
    SELECT * FROM cers_2223
       
  ),

  final AS (
    SELECT
      AceAssessmentId,
      AceAssessmentName,
      DistrictId,
      TestDistrictName,
      TestSchoolCdsCode,
      TestSchoolName,
      StateUniqueId,
      FirstName,
      LastSurname,
      AssessmentDate,
      TestSchoolYear,
      TestSessionId,
      AssessmentType,
      AssessmentSubType,
      AssessmentName,
      Subject,
      GradeLevelWhenAssessed AS GradeLevel,
      Completeness,
      AdministrationCondition,
      ScaleScoreAchievementLevel,
      ScaleScore,
      Alt1ScoreAchievementLevel,
      Alt2ScoreAchievementLevel,
      Claim1ScoreAchievementLevel,
      Claim2ScoreAchievementLevel,
      Claim3ScoreAchievementLevel,
      Claim4ScoreAchievementLevel
    FROM unioned
  )

SELECT * FROM final
