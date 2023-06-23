with 
  school_enr as (
    select
      Year,
      CdsCode,
      SchoolCode as EntityCode,
      'School' as EntityType,
      School as EntityName,
      'All Students' as Subgroup,
      'All Schools' as SchoolType,
      RaceEthnicCode,
      Gender,
      KDGN,
      GR_1,
      GR_2,
      GR_3,
      GR_4,
      GR_5,
      GR_6,
      GR_7,
      GR_8,
      UNGR_ELM,
      GR_9,
      GR_10,
      GR_11,
      GR_12,
      UNGR_SEC,
      ENR_TOTAL,
      ADULT
    from {{ ref('stg_RD__CdeEnr')}}
  ),

  entity_enr as (
    select
      cast(left(SchoolYear, 4) as int64) as Year,
      cast(null as string) as CdsCode,
      EntityCode,
      EntityType,
      EntityName,
      Subgroup,
      SchoolType,
      case
        when Ethnicity = 'Not Reported' then '0'
        when Ethnicity = 'American Indian or Alaska Native' then '1'
        when Ethnicity = 'Asian' then '2'
        when Ethnicity = 'Pacific Islander'  then '3'
        when Ethnicity = 'Filipino' then '4'
        when Ethnicity = 'Hispanic or Latino' then '5'
        when Ethnicity = 'African American' then '6'
        when Ethnicity = 'White' then '7'
        when Ethnicity = 'Two or More Races' then '9'
        else 'ERROR'
      end as RaceEthnicCode,
      'All' as Gender,
      GR_K as KDGN,
      GR_1,
      GR_2,
      GR_3,
      GR_4,
      GR_5,
      GR_6,
      GR_7,
      GR_8,
      UNGR_ELEM as UNGR_ELM,
      GR_9,
      GR_10,
      GR_11,
      GR_12,
      UNGR_SEC,
      ENR_TOTAL,
      cast(null as int64) as ADULT
    from {{ ref('stg_GSD__CdeEnrByRaceAndGradeEntities')}}
  ),

  unioned as (
    select * from school_enr
    union all
    select * from entity_enr
  ),

  final as (
    select
      Year,
      CdsCode,
      EntityCode,
      EntityType,
      EntityName,
      Subgroup,
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
        else 'ERROR'
      end as RaceEthnicity, 
      Gender,
      KDGN,
      GR_1,
      GR_2,
      GR_3,
      GR_4,
      GR_5,
      GR_6,
      GR_7,
      GR_8,
      UNGR_ELM,
      GR_9,
      GR_10,
      GR_11,
      GR_12,
      UNGR_SEC,
      ENR_TOTAL,
      ADULT
    from unioned
  )

SELECT *
FROM final
ORDER BY Year, EntityCode, RaceEthnicity, Gender