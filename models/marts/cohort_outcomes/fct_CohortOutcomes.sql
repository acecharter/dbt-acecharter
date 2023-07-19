with outcomes as (
    select
        *,
        case
            when EntityType = 'State' then '00'
            when EntityType = 'County' then CountyCode
            when EntityType = 'District' then DistrictCode
            when EntityType = 'School' then SchoolCode
        end as EntityCode
    from {{ ref('stg_RD__CdeAdjustedCohortOutcomes') }}
),

reg_grad as (
    select
        AcademicYear,
        EntityCode,
        CharterSchool,
        DASS,
        ReportingCategory,
        'Cohort Outcome' as OutcomeType,
        CohortStudents as OutcomeDenominator,
        'Regular HS Diploma' as Outcome,
        RegularHsDiplomaGraduatesCount as OutcomeCount
    from outcomes
),

met_uc_csu as (
    select
        AcademicYear,
        EntityCode,
        CharterSchool,
        DASS,
        ReportingCategory,
        'Cohort Graduate Outcome' as OutcomeType,
        RegularHsDiplomaGraduatesCount as OutcomeDenominator,
        'Met UC/CSU Grad Requirements' as Outcome,
        MetUcCsuGradReqsCount as OutcomeCount
    from outcomes
),

seal_of_biliteracy as (
    select
        AcademicYear,
        EntityCode,
        CharterSchool,
        DASS,
        ReportingCategory,
        'Cohort Graduate Outcome' as OutcomeType,
        RegularHsDiplomaGraduatesCount as OutcomeDenominator,
        'Seal of Biliteracy' as Outcome,
        SealOfBiliteracyCount as OutcomeCount
    from outcomes
),

golden_state_seal as (
    select
        AcademicYear,
        EntityCode,
        CharterSchool,
        DASS,
        ReportingCategory,
        'Cohort Graduate Outcome' as OutcomeType,
        RegularHsDiplomaGraduatesCount as OutcomeDenominator,
        'Golden State Seal Merit Diploma' as Outcome,
        GoldenStateSealMeritDiplomaCount as OutcomeCount
    from outcomes
),

chspe_competer as (
    select
        AcademicYear,
        EntityCode,
        CharterSchool,
        DASS,
        ReportingCategory,
        'Cohort Outcome' as OutcomeType,
        CohortStudents as OutcomeDenominator,
        'CHSPE Completer' as Outcome,
        ChspeCompleterCount as OutcomeCount
    from outcomes
),

adult_ed as (
    select
        AcademicYear,
        EntityCode,
        CharterSchool,
        DASS,
        ReportingCategory,
        'Cohort Outcome' as OutcomeType,
        CohortStudents as OutcomeDenominator,
        'Adult Ed HS Diploma' as Outcome,
        AdultEdHsDiplomaCount as OutcomeCount
    from outcomes
),

sped as (
    select
        AcademicYear,
        EntityCode,
        CharterSchool,
        DASS,
        ReportingCategory,
        'Cohort Outcome' as OutcomeType,
        CohortStudents as OutcomeDenominator,
        'SPED Certificate' as Outcome,
        SpedCertificateCount as OutcomeCount
    from outcomes
),

ged as (
    select
        AcademicYear,
        EntityCode,
        CharterSchool,
        DASS,
        ReportingCategory,
        'Cohort Outcome' as OutcomeType,
        CohortStudents as OutcomeDenominator,
        'GED Completer' as Outcome,
        GedCompleterCount as OutcomeCount
    from outcomes
),

other as (
    select
        AcademicYear,
        EntityCode,
        CharterSchool,
        DASS,
        ReportingCategory,
        'Cohort Outcome' as OutcomeType,
        CohortStudents as OutcomeDenominator,
        'Other Transfer' as Outcome,
        OtherTransferCount as OutcomeCount
    from outcomes
),

dropout as (
    select
        AcademicYear,
        EntityCode,
        CharterSchool,
        DASS,
        ReportingCategory,
        'Cohort Outcome' as OutcomeType,
        CohortStudents as OutcomeDenominator,
        'Dropout' as Outcome,
        DropoutCount as OutcomeCount
    from outcomes
),

still_enrolled as (
    select
        AcademicYear,
        EntityCode,
        CharterSchool,
        DASS,
        ReportingCategory,
        'Cohort Outcome' as OutcomeType,
        CohortStudents as OutcomeDenominator,
        'Still Enrolled' as Outcome,
        StillEnrolledCount as OutcomeCount
    from outcomes
),

unioned as (
    select * from reg_grad
    union all
    select * from met_uc_csu
    union all
    select * from seal_of_biliteracy
    union all
    select * from golden_state_seal
    union all
    select * from chspe_competer
    union all
    select * from adult_ed
    union all
    select * from sped
    union all
    select * from ged
    union all
    select * from other
    union all
    select * from dropout
    union all
    select * from still_enrolled
),

final as (
    select
        *,
        round(OutcomeCount / OutcomeDenominator, 4) as OutcomeRate
    from unioned
    where OutcomeDenominator > 0
    order by 1, 2, 3, 4, 5, 6, 7
)


select * from final
