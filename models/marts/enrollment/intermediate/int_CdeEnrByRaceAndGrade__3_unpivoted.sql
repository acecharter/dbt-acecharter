with unpivoted as (
  {{ dbt_utils.unpivot(
    relation=ref('int_CdeEnrByRaceAndGrade__2_filtered'),
    cast_to='INT64',
    exclude=[
      'Year',
      'EntityCode',
      'EntityType',
      'EntityName',
      'Subgroup',
      'SchoolType',
      'RaceEthnicCode',
      'Gender'
    ],
    remove=[
      'CdsCode',
      'ENR_TOTAL'
    ],
    field_name='GradeLevel',
    value_name='Enrollment'
  ) }}
),

enr_final as (
  select
    concat(
      cast(Year as STRING),
      EntityCode,
      RaceEthnicCode,
      Gender,
      Subgroup,
      SchoolType
    ) as UniqueEnrId,
    Year,
    concat(cast(Year as STRING), '-', format("%02d", Year - 1999)) as SchoolYear,
    EntityCode,
    EntityType,
    EntityName,
    SchoolType,
    RaceEthnicCode,
    case
      when RaceEthnicCode = '0' then 'Not Reported'
      when RaceEthnicCode = '1' then 'American Indian or Alaska Native'
      when RaceEthnicCode = '2' then 'Asian'
      when RaceEthnicCode = '3' then 'Pacific Islander'
      when RaceEthnicCode = '4' then 'Filipino'
      when RaceEthnicCode = '5' then 'Hispanic or Latino'
      when RaceEthnicCode = '6' then 'African American'
      when RaceEthnicCode = '7' then 'White'
      when RaceEthnicCode = '8' then 'Multiple or No Response (pre-2009)'
      when RaceEthnicCode = '9' then 'Two or More Races'
    end as RaceEthnicity, 
    Gender,
    case
      when GradeLevel = 'KDGN' then 'K'
      when GradeLevel = 'GR_1' then '1'
      when GradeLevel = 'GR_2' then '2'
      when GradeLevel = 'GR_3' then '3'
      when GradeLevel = 'GR_4' then '4'
      when GradeLevel = 'GR_5' then '5'
      when GradeLevel = 'GR_6' then '6'
      when GradeLevel = 'GR_7' then '7'
      when GradeLevel = 'GR_8' then '8'
      when GradeLevel = 'GR_9' then '9'
      when GradeLevel = 'GR_10' then '10'
      when GradeLevel = 'GR_11' then '11'
      when GradeLevel = 'GR_12' then '12'
      when GradeLevel = 'UNGR_ELM' then 'Ungraded Elementary'
      when GradeLevel = 'UNGR_SEC' then 'Ungraded Secondary'
      when GradeLevel = 'ADULT' then 'Adult'
    end as GradeLevel,
    Enrollment
  from unpivoted
  where
    Enrollment is not null
    and Enrollment > 0
),

total as (
  select
    Year,
    EntityCode,
    SchoolType,
    sum(ENR_TOTAL) as Enrollment
  from {{ ref('int_CdeEnrByRaceAndGrade__2_filtered')}}
  where Subgroup = 'All Students'
  group by 1, 2, 3
),

final as (
  select
    e.*,
    round(e.Enrollment / t.Enrollment, 4) as PctOfTotalEnrollment
  from enr_final as e
  left join total as t
  on
    e.Year = t.Year
    and e.EntityCode = t.EntityCode
    and e.SchoolType = t.SchoolType
)

select * 
from final
order by
  Year, 
  EntityCode, 
  RaceEthnicCode, 
  Gender, 
  GradeLevel