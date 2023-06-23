with
  elpi_levels as (
    select * from {{ ref('stg_GSD__ElpiLevels') }}
  ),
  
  caaspp_min_met_scores as (
    select
      AceAssessmentId,
      cast(GradeLevel as INT64) as GradeLevel,
      MinStandardMetScaleScore
    from {{ ref('fct_CaasppMinMetScaleScores') }}
    where
      Area='Overall'
      AND AceAssessmentId IN ('1', '2')
  ),

  cers as (
    select 
      AceAssessmentId,
      AceAssessmentName,
      TestDistrictId,
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
      GradeLevelWhenAssessed,
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
    from {{ ref('stg_RD__Cers')}}
  ),

  final as (
    select
      cers.*,
      e.ElpiLevel,
      cers.ScaleScore - c.MinStandardMetScaleScore as DistanceFromStandard,
      concat(cers.AssessmentName, '-', cers.StateUniqueId, '-', cers.AssessmentDate) as AssessmentId,
    from cers
    LEFT JOIN elpi_levels as e
      on
        cers.AceAssessmentId = e.AceAssessmentId
        AND cers.GradeLevelWhenAssessed = e.GradeLevel
        AND cast(cers.ScaleScore as INT64) BETWEEN cast(e.MinScaleScore as INT64) AND cast(e.MaxScaleScore as INT64)
    LEFT JOIN caaspp_min_met_scores as c
      on
        cers.AceAssessmentId = c.AceAssessmentId
        AND cers.GradeLevelWhenAssessed = c.GradeLevel 
    where cers.Completeness='Complete'
  )

select * from final