with
  ap as (
    select * from {{ ref('int_Ap__unpivoted') }}
  ),

  students as (
    select distinct * EXCEPT(SourceFileYear, FieldName, TestNumber, ValueName)
    from ap
  ),

  assessment_ids as (
    select 
        AceAssessmentId,
        AssessmentNameShort as AceAssessmentName,
        AssessmentSubject,
        SystemOrVendorAssessmentId as ExamCode
    from {{ ref('stg_GSD__Assessments') }}
    where 
        AssessmentFamilyNameShort = 'AP'
  ),

  admin_years as (
    select
      SourceFileYear,
      ApId,
      TestNumber,
      ValueName
    from ap
    where FieldName = 'AdminYear'
  ),

  exam_codes as (
    select
      SourceFileYear,
      ApId,
      TestNumber,
      ValueName
    from ap
    where FieldName = 'ExamCode'
  ),

  exam_grades as (
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
  
  exam_names AS (
    SELECT * FROM {{ ref('stg_RD__ApExamCodes')}}
  ),

  results as (
    select distinct
      admin_years.ApId,
      admin_years.TestNumber,
      admin_years.ValueName as AdminYear,
      concat(cast(1999 + cast(admin_years.ValueName as int64) as string), '-', admin_years.ValueName) as AssessmentSchoolYear,
      exam_codes.ValueName as ExamCode,
      exam_names.ExamName,
      exam_grades.ValueName as ExamGrade,
      irregularity_code_1.ValueName as IrregularityCode1,
      irregularity_code_2.ValueName as IrregularityCode2
    from admin_years
    left join exam_codes
    on admin_years.ApId = exam_codes.ApId
    and admin_years.TestNumber = exam_codes.TestNumber
    and admin_years.SourceFileYear = exam_codes.SourceFileYear
    left join exam_grades
    on admin_years.ApId = exam_grades.ApId
    and admin_years.TestNumber = exam_grades.TestNumber
    and admin_years.SourceFileYear = exam_grades.SourceFileYear
    left join irregularity_code_1
    on admin_years.ApId = irregularity_code_1.ApId
    and admin_years.TestNumber = irregularity_code_1.TestNumber
    and admin_years.SourceFileYear = irregularity_code_1.SourceFileYear
    left join irregularity_code_2
    on admin_years.ApId = irregularity_code_2.ApId
    and admin_years.TestNumber = irregularity_code_2.TestNumber
    and admin_years.SourceFileYear = irregularity_code_2.SourceFileYear
    left join exam_names
    on exam_codes.ValueName = exam_names.ExamCode
  ),

  final as (
      select distinct
        assessment_ids.AceAssessmentId,
        assessment_ids.AceAssessmentName,
        assessment_ids.AssessmentSubject,     
        students.*,
        results.* EXCEPT(ApId)
      from students
      left join results
      on  students.ApId = results.ApId
      left join assessment_ids
      on results.ExamCode = assessment_ids.ExamCode
  )

select * from final
