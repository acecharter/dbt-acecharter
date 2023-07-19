with elpac as (
    select * from {{ ref('int_Elpac__2_unpivoted') }}
),

demographics as (
    select * from {{ ref('stg_RD__ElpacStudentGroups') }}
),

final as (
    select
        elpac.EntityCode,
        elpac.EntityType,
        elpac.EntityName,
        elpac.EntityNameMid,
        elpac.EntityNameShort,
        elpac.RecordType,
        elpac.TestYear,
        elpac.SchoolYear,
        elpac.StudentGroupId,
        demographics.StudentGroupName as StudentGroup,
        elpac.GradeLevel,
        elpac.AssessmentType,
        elpac.AssessmentObjective,
        elpac.TotalEnrolled,
        elpac.TotalTestedWithScores,
        elpac.ReportingMethod,
        elpac.ResultDataType,
        elpac.SchoolResult,
        elpac.StudentWithResultCount
    from elpac
    left join demographics
        on elpac.StudentGroupId = demographics.StudentGroupId
)

select * from final
