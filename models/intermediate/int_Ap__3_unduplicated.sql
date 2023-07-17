with ap as (
    select
        concat(StateUniqueId,'-',AssessmentYear,'-',ExamCode) as ResultUniqueId,
        * except(SourceFileYear, TestNumber)
    from {{ ref('int_Ap__2_repivoted') }}
),

current_year_results as (
    select *
    from ap
    where CurrentYearScore = 'Yes'
),

other_previous_results as (
    select distinct *
    from ap
    where CurrentYearScore = 'No'
    and ResultUniqueId not in (
        select ResultUniqueId from current_year_results
    )
),

unduplicated_results as (
    select * from current_year_results
    union all
    select * from other_previous_results
),

assessment_ids as (
    select 
        AceAssessmentId,
        AssessmentNameShort as AceAssessmentName,
        AssessmentSubject,
        SystemOrVendorAssessmentId as ExamCode
    from {{ ref('stg_GSD__Assessments') }}
    where AssessmentFamilyNameShort = 'AP'
),

final as (
    select
        assessment_ids.AceAssessmentId,
        assessment_ids.AceAssessmentName,
        assessment_ids.AssessmentSubject,
        unduplicated_results.*
    from unduplicated_results
    left join assessment_ids
    on unduplicated_results.ExamCode = assessment_ids.ExamCode
)

select * from final
order by StateUniqueId, AceAssessmentName, AssessmentYear desc
