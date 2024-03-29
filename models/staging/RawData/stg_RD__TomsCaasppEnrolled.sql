with caaspp as (
    select * from {{ ref('base_RD__TomsCaasppEnrolledEmpower')}}
    union all select * from {{ ref('base_RD__TomsCaasppEnrolledEsperanza')}}
    union all select * from {{ ref('base_RD__TomsCaasppEnrolledInspire')}}
    union all select * from {{ ref('base_RD__TomsCaasppEnrolledHighSchool')}}
),

race_ethnicity as (
    select * from {{ ref('stg_RD__TomsEthnicityCodes')}}
),

assessment_ids as (
    select 
        AceAssessmentId,
        AssessmentNameShort as AceAssessmentName,
        AssessmentSubject,
        SystemOrVendorAssessmentId
    from {{ ref('stg_GSD__Assessments') }}
    where SystemOrVendorName = 'CAASPP'
),

caaspp_yrs_id_added as (
    select
        cast(extract(year from FinalTestCompletedDate) as int64) as TestYear,
        concat(cast(cast(extract(year from FinalTestCompletedDate) - 1 as int64) as string),'-', cast(cast(extract(year from FinalTestCompletedDate) - 2000 as int64) as string)) as SchoolYear,
        a.AceAssessmentId,
        a.AceAssessmentName,
        a.AssessmentSubject,
        c.*
    from caaspp as c
    left join assessment_ids as a
    on c.RecordType = a.SystemOrVendorAssessmentId
),

min_met_scores as (
    select
        AceAssessmentId,
        GradeLevel,
        MinStandardMetScaleScore
    from {{ ref('fct_CaasppMinMetScaleScores') }}
    where Area='Overall'
),

caaspp_id_race_added as (
    select
        c.*,
        r.RaceEthnicity
    from caaspp_yrs_id_added as c
    left join race_ethnicity as r
    on c.ReportingEthnicity = r.RaceEthnicityCode
),

final as (
    select
        c.*,
        case when ELStatus is true or date(RFEPDate) > date(TestYear - 4, 6, 15) then true else false end as ElWithinPast4Years,
        cast(
            case when c.ScaleScore is not null then round(c.ScaleScore - m.MinStandardMetScaleScore, 0) else null end as int64
        ) as Dfs,
        cast(
            case when c.ScaleScoreMinus1 is not null then round(c.ScaleScoreMinus1 - m1.MinStandardMetScaleScore, 0) else null end as int64
        ) as DfsMinus1,
        cast(
            case when c.ScaleScoreMinus2 is not null then round(c.ScaleScoreMinus2 - m2.MinStandardMetScaleScore, 0) else null end as int64
        ) as DfsMinus2,
        cast(
            case when c.ScaleScoreMinus3 is not null then round(c.ScaleScoreMinus3 - m3.MinStandardMetScaleScore, 0) else null end as int64
        ) as DfsMinus3,
    from caaspp_id_race_added as c
    left join min_met_scores as m
    on
        c.AceAssessmentId = m.AceAssessmentId
        and c.GradeAssessed = m.GradeLevel
    left join min_met_scores as m1
    on
        c.AceAssessmentId = m1.AceAssessmentId
        and c.GradeAssessedMinus1 = m1.GradeLevel
    left join min_met_scores as m2
    on
        c.AceAssessmentId = m2.AceAssessmentId
        and c.GradeAssessedMinus2 = m2.GradeLevel
    left join min_met_scores as m3
    on
        c.AceAssessmentId = m3.AceAssessmentId
        and c.GradeAssessedMinus3 = m3.GradeLevel
)

select * from final
