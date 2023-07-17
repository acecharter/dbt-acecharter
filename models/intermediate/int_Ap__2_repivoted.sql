with ap as (
    select * from {{ ref('int_Ap__1_unpivoted') }}
),

students as (
    select distinct * EXCEPT(FieldName, TestNumber, ValueName)
    from ap
),

admin_year as (
    select
        SourceFileYear,
        ApId,
        TestNumber,
        ValueName
    from ap
    where FieldName = 'AdminYear'
),

exam_code as (
    select
        SourceFileYear,
        ApId,
        TestNumber,
        ValueName
    from ap
    where FieldName = 'ExamCode'
),

exam_grade as (
    select
        SourceFileYear,
        ApId,
        TestNumber,
        ValueName
    from ap
    where FieldName = 'ExamGrade'
),

irregularity_code_1 as (
    select
        SourceFileYear,
        ApId,
        TestNumber,
        ValueName
    from ap
    where FieldName = 'IrregularityCode1'
),

irregularity_code_2 as (
    select
        SourceFileYear,
        ApId,
        TestNumber,
        ValueName
    from ap
    where FieldName = 'IrregularityCode2'
),

results as (
    select    
        admin_year.SourceFileYear,
        case when admin_year.SourceFileYear = cast(concat('20', admin_year.ValueName) as int64) then 'Yes' else 'No' end as CurrentYearScore,
        admin_year.ApId,
        admin_year.TestNumber,
        cast(concat('20', admin_year.ValueName) as int64) as AssessmentYear,
        concat(cast(1999 + cast(admin_year.ValueName as int64) as string), '-', admin_year.ValueName) as AssessmentSchoolYear,
        exam_code.ValueName as ExamCode,
        exam_grade.ValueName as ExamGrade,
        irregularity_code_1.ValueName as IrregularityCode1,
        irregularity_code_2.ValueName as IrregularityCode2
    from admin_year
    left join exam_code
    on admin_year.ApId = exam_code.ApId
    and admin_year.TestNumber = exam_code.TestNumber
    and admin_year.SourceFileYear = exam_code.SourceFileYear
    left join exam_grade
    on admin_year.ApId = exam_grade.ApId
    and admin_year.TestNumber = exam_grade.TestNumber
    and admin_year.SourceFileYear = exam_grade.SourceFileYear
    left join irregularity_code_1
    on admin_year.ApId = irregularity_code_1.ApId
    and admin_year.TestNumber = irregularity_code_1.TestNumber
    and admin_year.SourceFileYear = irregularity_code_1.SourceFileYear
    left join irregularity_code_2
    on admin_year.ApId = irregularity_code_2.ApId
    and admin_year.TestNumber = irregularity_code_2.TestNumber
    and admin_year.SourceFileYear = irregularity_code_2.SourceFileYear
),
  
exam_names AS (
    SELECT * FROM {{ ref('stg_RD__ApExamCodes')}}
),

final as (
    select
        students.SourceFileYear,
        results.CurrentYearScore,
        results.TestNumber,
        students.ApId,
        students.StateUniqueId,
        students.StudentUniqueId,
        students.LastName,
        students.FirstName,
        students.MiddleInitial,
        students.Gender,
        students.DateOfBirth,
        case when results.CurrentYearScore = 'Yes' then students.GradeLevel else null end as GradeLevel,
        case when results.CurrentYearScore = 'Yes' then students.AiCode else null end as AiCode,
        case when results.CurrentYearScore = 'Yes' then students.AiInstitutionName else null end as AiInstitutionName,
        case when results.CurrentYearScore = 'Yes' then students.StudentIdentifier else null end as StudentIdentifier,
        case when results.CurrentYearScore = 'Yes' then students.RaceEthnicity else null end as RaceEthnicity,
        results.AssessmentYear,
        results.AssessmentSchoolYear,
        results.ExamCode,
        exam_names.ExamName,
        results.ExamGrade,
        results.IrregularityCode1,
        results.IrregularityCode2
    from students
    left join results
    on  students.ApId = results.ApId 
    and students.SourceFileYear = results.SourceFileYear
    left join exam_names
    on exam_names.ExamCode = results.ExamCode
)

select * from final
