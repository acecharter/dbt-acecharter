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
        case
            when AceAssessmentId = '1' then 'SB ELA Summative'
            when AceAssessmentId = '2' then 'SB Math Summative'
            when AceAssessmentId = '3' then 'CAA ELA'
            when AceAssessmentId = '4' then 'CAA Math'
            when AceAssessmentId = '5' then 'CAA Science'
            when AceAssessmentId = '6' then 'CAST'
            when AceAssessmentId = '7' then 'CSA'
            when AceAssessmentId = '8' then 'Summative ELPAC'
            when AceAssessmentId = '9' then 'Initial ELPAC'
            when
                AceAssessmentId = '15'
                then
                    concat(
                        'SB ELA',
                        regexp_extract(AssessmentName, '.+(\\s\\-\\s.+)')
                    )
            when
                AceAssessmentId = '16'
                then
                    concat(
                        'SB Math',
                        regexp_extract(AssessmentName, '.+(\\s\\-\\s.+)')
                    )
            when AceAssessmentId = '17' then 'SB ELA ICA'
            when AceAssessmentId = '18' then 'SB Math ICA'
        end as AssessmentName,
        case
            when AceAssessmentId in ('1', '3', '15', '17') then 'ELA'
            when AceAssessmentId in ('2', '4', '16', '18') then 'Math'
            when AceAssessmentId in ('5', '6') then 'Science'
            when AceAssessmentId in ('7') then 'Spanish'
            when AceAssessmentId in ('8', '9') then 'English Fluency'
        end as AssessmentSubject,
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
