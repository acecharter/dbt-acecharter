with
  ap as (
    select * from {{ ref('int_Ap2022__unpivoted') }}
  ),

  students as (
    select distinct * EXCEPT(FieldName, TestNumber, ValueName)
    from ap
  ),

  ay as (
    select
      ApId,
      TestNumber,
      ValueName
    from ap
    where FieldName = 'AdminYear'
  ),

  ec as (
    select
      ApId,
      TestNumber,
      ValueName
    from ap
    where FieldName = 'ExamCode'
  ),

  eg as (
    select
      ApId,
      TestNumber,
      ValueName
    from ap
    where FieldName = 'ExamGrade'
  ),

  ic1 as (
    select
      ApId,
      TestNumber,
      ValueName
    from ap
    where FieldName = 'IrregularityCode1'
  ),

  ic2 as (
    select
      ApId,
      TestNumber,
      ValueName
    from ap
    where FieldName = 'IrregularityCode2'
  ),

  results as (
    select
      ay.ApId,
      ay.TestNumber,
      ay.ValueName as AdminYear,
      ec.ValueName as ExamCode,
      eg.ValueName as ExamGrade,
      ic1.ValueName as IrregularityCode1,
      ic2.ValueName as IrregularityCode2
    from ay
    left join ec
    on ay.ApId = ec.ApId
    and ay.TestNumber = ec.TestNumber
    left join eg
    on ay.ApId = eg.ApId
    and ay.TestNumber = eg.TestNumber
    left join ic1
    on ay.ApId = ic1.ApId
    and ay.TestNumber = ic1.TestNumber
    left join ic2
    on ay.ApId = ic2.ApId
    and ay.TestNumber = ic2.TestNumber
  ),

  final as (
      select
        s.*,
        r.* EXCEPT(ApId)
      from students as s
      left join results as r
      on  s.ApId = r.ApId 
  )

select * from final