{{ config(
    materialized='table'
)}}

with unpivoted as (
    {{ dbt_utils.unpivot(
        relation=ref('stg_RD__Ap'),
        cast_to='STRING',
        exclude=[
            'StateUniqueId',
            'StudentUniqueId',
            'SourceFileYear',
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
        * except (FieldName, ValueName),
        substr(FieldName, 1, length(FieldName)-2) as FieldName,
        substr(FieldName, length(FieldName)-1, 2) as TestNumber,
        ValueName
    from unpivoted
    where
        ValueName is not null
        and trim(ValueName) != ''
  )

select * from final
