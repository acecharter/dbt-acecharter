with
  ap as (
    select * {{ ref('int_Ap2022__unpivoted') }}
  ),

  ay as (
    select
      ApId,
      ValueName
    from ap
    where FieldName = 'AdminYear'
  ),

  ec as (
    select
      ApId,
      ValueName
    from ap
    where FieldName = 'ExamCode'
  ),

  eg as (
    select
      ApId,
      ValueName
    from ap
    where FieldName = 'ExamGrade'
  ),

  ic1 as (
    select
      ApId,
      ValueName
    from ap
    where FieldName = 'IrregularityCode1'
  ),

  ic2 as (
    select
      ApId,
      ValueName
    from ap
    where FieldName = 'IrregularityCode2'
  ),

  final as (
      select
        ap.* EXCEPT(FieldName, FieldNameGroup, ValueName),
        ay.ValueName as AdminYear,
        ec.ValueName as ExamCode,
        eg.ValueName as ExamGrade,
        ic1.ValueName as IrregularityCode1,
        ic2.ValueName as IrregularityCode2
      from ap
      left join ay
      on  o.ApId = ay.ApId
      left join ec
      on  o.ApId = ec.ApId
      left join eg
      on  o.ApId = eg.ApId
      left join ic1
      on  o.ApId = ic1.ApId
      left join ic2
      on  o.ApId = ic2.ApId 
  )

select * from final