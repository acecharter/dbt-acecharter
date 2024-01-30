with elpi_levels as (
    select * from {{ ref('stg_GSD__ElpiLevels') }}
),

caaspp_min_met_scores as (
    select
        AceAssessmentId,
        cast(GradeLevel as int64) as GradeLevel,
        MinStandardMetScaleScore
    from {{ ref('fct_CaasppMinMetScaleScores') }}
    where
        Area = 'Overall'
        and AceAssessmentId in ('1', '2')
),

cers as (
    select
        AceAssessmentId,
        AceAssessmentName,
        AceAssessmentSubject,
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
    from {{ ref('stg_RD__Cers') }}
),

final as (
    select
        cers.*,
        e.ElpiLevel,
        cers.ScaleScore - c.MinStandardMetScaleScore as DistanceFromStandard,
        concat(
            cers.AssessmentName,
            '-',
            cers.StateUniqueId,
            '-',
            cers.AssessmentDate
        ) as AssessmentId
    from cers
    left join elpi_levels as e
        on
            cers.AceAssessmentId = e.AceAssessmentId
            and cers.GradeLevelWhenAssessed = e.GradeLevel
            and cast(cers.ScaleScore as int64) between cast(
                e.MinScaleScore as int64
            ) and cast(e.MaxScaleScore as int64)
    left join caaspp_min_met_scores as c
        on
            cers.AceAssessmentId = c.AceAssessmentId
            and cers.GradeLevelWhenAssessed = c.GradeLevel
    where cers.Completeness = 'Complete'
)

select * from final
