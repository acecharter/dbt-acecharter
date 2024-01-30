with star as (
    select
        AceAssessmentId,
        AssessmentName,
        AssessmentSubject,
        StateUniqueId,
        TestedSchoolId,
        SchoolYear as AssessmentSchoolYear,
        AssessmentID as AssessmentId,
        cast(AssessmentDate as string) as AssessmentDate,
        GradeLevel as GradeLevelWhenAssessed,
        cast(AssessmentGradeLevel as string) as AssessmentGradeLevel,
        'Overall' as AssessmentObjective,
        ReportingMethod,
        StudentResultDataType,
        StudentResult
    from {{ ref('int_RenStar_unpivoted') }}
),

cers as (
    select
        AceAssessmentId,
        AssessmentName,
        AssessmentSubject,
        StateUniqueId,
        TestedSchoolId,
        SchoolYear as AssessmentSchoolYear,
        AssessmentId,
        cast(AssessmentDate as string) as AssessmentDate,
        GradeLevelWhenAssessed,
        AssessmentGradeLevel,
        AssessmentObjective,
        ReportingMethod,
        StudentResultDataType,
        StudentResult
    from {{ ref('int_Cers__2_unpivoted') }}
),

ap as (
    select
        AceAssessmentId,
        AceAssessmentName as AssessmentName,
        AssessmentSubject,
        StateUniqueId,
        case when AiCode = 54660 then '125617' else 'Unknown' end
            as TestedSchoolId,
        AssessmentSchoolYear,
        concat(AceAssessmentName, '-', StateUniqueId) as AssessmentId,
        cast(null as string) as AssessmentDate,
        case
            when
                GradeLevel not in ('9', '10', '11', '12')
                then cast(null as int64)
            else cast(GradeLevel as int64)
        end as GradeLevelWhenAssessed,
        GradeLevel as AssessmentGradeLevel,
        'Overall' as AssessmentObjective,
        'AP Score' as ReportingMethod,
        'INT64' as StudentResultDataType,
        cast(ExamGrade as string) as StudentResult
    from {{ ref('int_Ap__3_unduplicated') }}
),

unioned_results as (
    select * from star
    union all
    select * from cers
    union all
    select * from ap
),

final as (
    select
        AceAssessmentId,
        AssessmentName,
        StateUniqueId,
        TestedSchoolId,
        AssessmentSchoolYear,
        AssessmentId,
        AssessmentDate,
        GradeLevelWhenAssessed,
        AssessmentGradeLevel,
        AssessmentSubject,
        AssessmentObjective,
        ReportingMethod,
        StudentResultDataType,
        StudentResult
    from unioned_results
)

select * from final
