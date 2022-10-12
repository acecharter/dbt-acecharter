WITH
  elpi_levels AS (
    SELECT * FROM {{ ref('stg_GSD__ElpiLevels') }}
  ),
  
  caaspp_min_met_scores AS (
    SELECT
      AceAssessmentId,
      CAST(GradeLevel AS INT64) AS GradeLevel,
      MinStandardMetScaleScore
    FROM {{ ref('fct_CaasppMinMetScaleScores') }}
    WHERE
      Area='Overall'
      AND AceAssessmentId IN ('1', '2')
  ),

  cers AS (
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
    FROM {{ ref('stg_RD__Cers')}}
  ),

  final AS (
    SELECT
      cers.*,
      e.ElpiLevel,
      cers.ScaleScore - c.MinStandardMetScaleScore AS DistanceFromStandard
    FROM cers
    LEFT JOIN elpi_levels AS e
      ON
        cers.AceAssessmentId = e.AceAssessmentId
        AND CAST(cers.GradeLevel AS INT64) = e.GradeLevel
        AND CAST(cers.ScaleScore AS INT64) BETWEEN CAST(e.MinScaleScore AS INT64) AND CAST(e.MaxScaleScore AS INT64)
    LEFT JOIN caaspp_min_met_scores AS c
      ON
        cers.AceAssessmentId = c.AceAssessmentId
        AND cers.GradeLevel = c.GradeLevel
        
  )

SELECT * FROM final