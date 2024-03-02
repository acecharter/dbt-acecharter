with schools as (
    select * from {{ ref('dim_Schools')}}
),

caaspp_recent_rfep_and_el_excl_newcomers as (
    select *
    from {{ ref('stg_RD__TomsCaasppTested')}}
    where ElWithinPast4Years = 'Y'
    and ScoreStatus = 'V'
    and IncludeIndicator = 'Y'
    and FirstEntryDateInUSSchool <= date(concat(cast(TestYear - 1 as string),'-04-15'))
),

combined_schoolwide_agg as (
    select
        CALPADSSchoolCode,
        TestYear,
        SchoolYear,
        'EL within last 4 years' as DemographicName,
        '13' as GradeAssessed,
        RecordType,
        AssessmentSubject,
        round(sum(case when AchievementLevels>=3 then 1 else 0 end) / count(*), 3) as PercentMetOrAbove,
        round(avg(Dfs), 1) as AverageDfs,
        count(*) as StudentWithResultCount
    from caaspp_recent_rfep_and_el_excl_newcomers
    group by 1, 2, 3, 4, 5, 6, 7
),

combined_by_grade_level_agg as (
    select
        CALPADSSchoolCode,
        TestYear,
        SchoolYear,
        'EL within last 4 years' as DemographicName,
        GradeAssessed,
        RecordType,
        AssessmentSubject,
        round(sum(case when AchievementLevels>=3 then 1 else 0 end) / count(*), 3) as PercentMetOrAbove,
        round(avg(Dfs), 1) as AverageDfs,
        count(*) as StudentWithResultCount
    from caaspp_recent_rfep_and_el_excl_newcomers
    group by 1, 2, 3, 4, 5, 6, 7
),

recent_rfep_schoolwide_agg as (
    select
        CALPADSSchoolCode,
        TestYear,
        SchoolYear,
        'Recent RFEP (within last 4 years)' as DemographicName,
        '13' as GradeAssessed,
        RecordType,
        AssessmentSubject,
        round(sum(case when AchievementLevels>=3 then 1 else 0 end) / count(*), 3) as PercentMetOrAbove,
        round(avg(Dfs), 1) as AverageDfs,
        count(*) as StudentWithResultCount
    from caaspp_recent_rfep_and_el_excl_newcomers
    where RFEPDate is not null
    group by 1, 2, 3, 4, 5, 6, 7
),

recent_rfep_by_grade_level_agg as (
    select
        CALPADSSchoolCode,
        TestYear,
        SchoolYear,
        'Recent RFEP (within last 4 years)' as DemographicName,
        GradeAssessed,
        RecordType,
        AssessmentSubject,
        round(sum(case when AchievementLevels>=3 then 1 else 0 end) / count(*), 3) as PercentMetOrAbove,
        round(avg(Dfs), 1) as AverageDfs,
        count(*) as StudentWithResultCount
    from caaspp_recent_rfep_and_el_excl_newcomers
    where RFEPDate is not null
    group by 1, 2, 3, 4, 5, 6, 7
),

unioned_agg as (
    select * from combined_schoolwide_agg
    union all
    select * from combined_by_grade_level_agg
    union all
    select * from recent_rfep_schoolwide_agg
    union all
    select * from recent_rfep_by_grade_level_agg
),

dfs as (
    select * except(PercentMetOrAbove),
    'FLOAT64' as ResultDataType,
    'Mean Distance From Standard' as ReportingMethod,
    AverageDfs as SchoolResult
    from unioned_agg
),

pct_met_or_above as (
    select * except(PercentMetOrAbove),
    'FLOAT64' as ResultDataType,
    'Percent Met or Above' as ReportingMethod,
    PercentMetOrAbove as SchoolResult
    from unioned_agg
),

unioned as (
    select * from dfs
    union all
    select * from pct_met_or_above
),

final as (
    select
        unioned.CALPADSSchoolCode as EntityCode,
        'School' as EntityType,
        schools.SchoolName as EntityName,
        schools.SchoolNameMid as EntityNameMid,
        schools.SchoolNameShort as EntityNameShort,
        cast(null as string) as TypeId,
        unioned.TestYear,
        unioned.SchoolYear,
        cast(null as string) as DemographicId,
        'English-Language Fluency' as StudentGroup,
        unioned.DemographicName,
        unioned.GradeAssessed as GradeLevel,
        unioned.RecordType as TestId,
        unioned.AssessmentSubject as TestSubject,
        cast(null as int64) as StudentsEnrolled,
        unioned.StudentWithResultCount as StudentsWithScores,
        'Overall' as AssessmentObjective,
        unioned.ReportingMethod,
        unioned.ResultDataType,
        unioned.SchoolResult,
        unioned.StudentWithResultCount
    from unioned
    left join schools
    on unioned.CALPADSSchoolCode = schools.SchoolId
)

select * from final
