{{ config(
    materialized='table'
)}}

with assessment_ids as (
    select 
        AceAssessmentId,
        AssessmentNameShort as AceAssessmentName,
        AssessmentSubject as AceAssessmentSubject,
        case AssessmentNameShort
            when 'SB ELA Summative' then 'ELA SUM'
            when 'SB Math Summative' then 'Math SUM'
            when 'CAA ELA' then 'CAAELA SUM'
            when 'CAA Math' then 'CAAMATH SUM'
            when 'CAST' then 'CAST SUM'
            when 'CSA' then 'CSA SUM'
            when 'SB ELA IAB/FIAB' then 'ELA IAB'
            when 'SB Math IAB/FIAB' then 'Math IAB'
            when 'SB ELA ICA' then 'ELA ICA'
            when 'SB Math ICA' then 'Math ICA'
            when 'Summative ELPAC' then 'ELPAC SUM'
            when 'ALT ELPAC' then 'ALTELPAC SUM'
            else 'ERROR'
        end as SubjectAssessmentSubType
    from {{ ref('stg_GSD__Assessments') }}
    where 
        SystemOrVendorName = 'CAASPP' 
        or SystemOrVendorName = 'ELPAC'
),

cers_1819 as (
    select * from {{ ref('base_RD__Cers1819') }}
),

cers_1920 as (
    select * from {{ ref('base_RD__Cers1920') }}
),

cers_2021 as (
    select * from {{ ref('base_RD__Cers2021') }}
),

cers_2122 as (
    select * from {{ ref('base_RD__Cers2122') }}
),

cers_2223 as (
    select * from {{ ref('base_RD__Cers2223') }}
),

unioned as (
    select * from cers_1819 
    union all
    select * from cers_1920
    union all
    select * from cers_2021
    union all
    select * from cers_2122
    union all
    select * from cers_2223
            
),

final as (
    select distinct
        a.AceAssessmentId,
        a.AceAssessmentName,
        a.AceAssessmentSubject,
        u.DistrictId as TestDistrictId,
        u.DistrictName as TestDistrictName,
        u.SchoolId as TestSchoolCdsCode,
        u.SchoolName as TestSchoolName,
        u.StudentIdentifier as StateUniqueId,
        u.FirstName,
        u.LastOrSurname as LastSurname,
        DATE(u.SubmitDateTime) as AssessmentDate,
        cast(u.SchoolYear as int64) as TestSchoolYear,
        u.TestSessionId,
        u.AssessmentType,
        u.AssessmentSubType,
        u.AssessmentName,
        u.Subject,
        case u.GradeLevelwhenAssessed
            when 'IT' then -4
            when 'PR' then -3
            when 'PK' then -2
            when 'TK' then -1
            when 'KG' then 0
            when 'PS' then 14
            else safe_cast(u.GradeLevelwhenAssessed as int64)
        end as GradeLevelwhenAssessed,
        u.Completeness,
        u.AdministrationCondition,
        safe_cast(u.ScaleScoreAchievementLevel as int64) as ScaleScoreAchievementLevel,
        safe_cast(u.ScaleScore as int64) as ScaleScore,
        nullif(u.Alt1ScoreAchievementLevel,'') as Alt1ScoreAchievementLevel,
        nullif(u.Alt2ScoreAchievementLevel,'') as Alt2ScoreAchievementLevel,
        nullif(u.Claim1ScoreAchievementLevel,'') as Claim1ScoreAchievementLevel,
        nullif(u.Claim2ScoreAchievementLevel,'') as Claim2ScoreAchievementLevel,
        nullif(u.Claim3ScoreAchievementLevel,'') as Claim3ScoreAchievementLevel,
        nullif(u.Claim4ScoreAchievementLevel,'') as Claim4ScoreAchievementLevel
    from unioned as u
    left join assessment_ids as a
    on concat(u.Subject, ' ', u.AssessmentSubType) = a.SubjectAssessmentSubType
)

select * from final
