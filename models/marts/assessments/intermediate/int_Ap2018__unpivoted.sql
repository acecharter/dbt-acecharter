{{ config(
    materialized='table'
)}}

with unpivoted as (
    {{ dbt_utils.unpivot(
      relation=ref('stg_RD__Ap2018'),
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

  final as (
    select
      * EXCEPT(FieldName, ValueName),
      SUBSTR(FieldName, 1, LENGTH(FieldName)-2) as FieldName,
      SUBSTR(FieldName, LENGTH(FieldName)-1, 2) as TestNumber,
      ValueName
    from unpivoted
    where
      ValueName is not null
      and trim(ValueName) != ''
  )

select * from final