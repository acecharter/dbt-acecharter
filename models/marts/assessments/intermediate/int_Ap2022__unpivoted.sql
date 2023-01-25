{{ config(
    materialized='table'
)}}

with unpivoted as (
    {{ dbt_utils.unpivot(
      relation=ref('stg_RD__Ap2022'),
      cast_to='STRING',
      exclude=[
        'ApId',
        'LastName',
        'FirstName',
        'MiddleInitial',
        'Gender',
        'DateOfBirth',
        'GradeLevel',
        'AiCode',
        'AiInstitutionName',
        'StudentIdentifier',
        'RaceEthnicity'
      ],
      remove=[],
      field_name='FieldName',
      value_name='ValueName'
    ) }}
  ),

  filtered as (
    select
      * EXCEPT(FieldName, ValueName),
      SUBSTR(FieldName, 1, LENGTH(FieldName)-2) as FieldName,
      SUBSTR(FieldName, LENGTH(FieldName)-2, 2) as FieldNameGroup,
      ValueName
    from unpivoted
    where
      ValueName is not null
      and trim(ValueName) != ''
  ),

  ay as (
    select
      ApId,
      ValueName as AdminYear
    from filtered
    where FieldName = 'AdminYear'
  ),

  ec as (
    select
      ApId,
      ValueName as ExamCode
    from filtered
    where FieldName = 'ExamCode'
  ),

  eg as (
    select
      ApId,
      ValueName as ExamGrade
    from filtered
    where FieldName = 'ExamGrade'
  ),

  ic1 as (
    select
      ApId,
      ValueName as IrregularityCode1
    from filtered
    where FieldName = 'IrregularityCode1'
  ),

  ic2 as (
    select
      ApId,
      ValueName as IrregularityCode1
    from filtered
    where FieldName = 'IrregularityCode2'
  ),

  final as (
      select
        o.* EXCEPT(FieldName, FieldNameGroup, ValueName),
        ay.AdminYear,
        ec.ExamCode,
        eg.ExamGrade,
        ic1.IrregularityCode1,
        ic2.IrregularityCode2
      from filtered as o
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
limit 11111111111111