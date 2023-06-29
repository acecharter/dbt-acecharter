with
  ap as (
    select * from {{ ref('int_Ap__unpivoted') }}
  ),

  students as (
    select distinct * EXCEPT(FieldName, TestNumber, ValueName)
    from ap
  ),

  
  assessment_ids as (
    select 
        AceAssessmentId,
        AssessmentNameShort as AceAssessmentName,
        SystemOrVendorAssessmentId as ExamCode
    from {{ ref('stg_GSD__Assessments') }}
    where 
        AssessmentFamilyNameShort = 'AP'
  ),

  ay as (
    select
      SourceFileYear,
      ApId,
      TestNumber,
      ValueName
    from ap
    where FieldName = 'AdminYear'
  ),

  ec as (
    select
      SourceFileYear,
      ApId,
      TestNumber,
      ValueName
    from ap
    where FieldName = 'ExamCode'
  ),

  eg as (
    select
      SourceFileYear,
      ApId,
      TestNumber,
      ValueName
    from ap
    where FieldName = 'ExamGrade'
  ),

  ic1 as (
    select
      SourceFileYear,
      ApId,
      TestNumber,
      ValueName
    from ap
    where FieldName = 'IrregularityCode1'
  ),

  ic2 as (
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
    select     
      ay.SourceFileYear,
      ay.ApId,
      ay.TestNumber,
      ay.ValueName as AdminYear,
      ec.ValueName as ExamCode,
      en.ExamName,
      eg.ValueName as ExamGrade,
      ic1.ValueName as IrregularityCode1,
      ic2.ValueName as IrregularityCode2
    from ay
    left join ec
    on ay.ApId = ec.ApId
    and ay.TestNumber = ec.TestNumber
    and ay.SourceFileYear = ec.SourceFileYear
    left join eg
    on ay.ApId = eg.ApId
    and ay.TestNumber = eg.TestNumber
    and ay.SourceFileYear = eg.SourceFileYear
    left join ic1
    on ay.ApId = ic1.ApId
    and ay.TestNumber = ic1.TestNumber
    and ay.SourceFileYear = ic1.SourceFileYear
    left join ic2
    on ay.ApId = ic2.ApId
    and ay.TestNumber = ic2.TestNumber
    and ay.SourceFileYear = ic2.SourceFileYear
    left join exam_names as en
    on ec.ValueName = en.ExamCode
  ),

  final as (
      select
        assessment_ids.AceAssessmentId,
        assessment_ids.AceAssessmentName,
        assessment_ids.AssessmentSubject,
        case when s.SourceFileYear = CAST(AdminYear as INT64) + 2000 then 'Yes' else 'No' end as AdminYrEqualsSourceFileYr,        
        s.*,
        r.* EXCEPT(SourceFileYear, ApId)
      from students as s
      left join results as r
      on  s.ApId = r.ApId 
      and s.SourceFileYear = r.SourceFileYear
      left join assessment_ids
      on r.ExamCode = assessment_ids.ExamCode
  )

select * from final