with caaspp as (
    select * from {{ ref('int_Caaspp__2_unpivoted') }}
    union all
    select * from {{ ref('int_Cast__2_unpivoted') }}
),


demographics as (
    select *
    from {{ ref('stg_RD__CaasppStudentGroups') }}
),

final as (
    select
        caaspp.EntityCode,
        caaspp.EntityType,
        caaspp.EntityName,
        caaspp.EntityNameMid,
        caaspp.EntityNameShort,
        caaspp.TypeId,
        caaspp.TestYear,
        caaspp.SchoolYear,
        caaspp.DemographicId,
        demographics.StudentGroup,
        demographics.DemographicName,
        caaspp.GradeLevel,
        caaspp.TestId,
        caaspp.AssessmentSubject as TestSubject,
        caaspp.StudentsEnrolled,
        caaspp.StudentsWithScores,
        caaspp.AssessmentObjective,
        caaspp.ReportingMethod,
        caaspp.ResultDataType,
        caaspp.SchoolResult,
        caaspp.StudentWithResultCount
    from caaspp
    left join demographics
        on caaspp.DemographicId = demographics.DemographicId
)

select * from final
