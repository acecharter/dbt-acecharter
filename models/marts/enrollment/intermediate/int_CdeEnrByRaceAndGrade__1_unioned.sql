WITH 
  school_enr AS (
    SELECT
      Year,
      CdsCode,
      SchoolCode AS EntityCode,
      'School' AS EntityType,
      School AS EntityName,
      'All Students' AS Subgroup,
      'All Schools' AS SchoolType,
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
    FROM {{ ref('stg_RD__CdeEnr')}}
  ),

  entity_enr AS (
    SELECT
      CAST(LEFT(SchoolYear, 4) AS INT64) AS Year,
      CAST(NULL AS STRING) AS CdsCode,
      EntityCode,
      EntityType,
      EntityName,
      Subgroup,
      SchoolType,
      CASE
        WHEN Ethnicity = 'Not Reported' THEN '0'
        WHEN Ethnicity = 'American Indian or Alaska Native' THEN '1'
        WHEN Ethnicity = 'Asian' THEN '2'
        WHEN Ethnicity = 'Pacific Islander'  THEN '3'
        WHEN Ethnicity = 'Filipino' THEN '4'
        WHEN Ethnicity = 'Hispanic or Latino' THEN '5'
        WHEN Ethnicity = 'African American' THEN '6'
        WHEN Ethnicity = 'White' THEN '7'
        WHEN Ethnicity = 'Two or More Races' THEN '9'
      END AS RaceEthnicCode,
      'All' AS Gender,
      GR_K,
      GR_1,
      GR_2,
      GR_3,
      GR_4,
      GR_5,
      GR_6,
      GR_7,
      GR_8,
      UNGR_ELEM,
      GR_9,
      GR_10,
      GR_11,
      GR_12,
      UNGR_SEC,
      ENR_TOTAL,
      CAST(NULL AS INT64) AS ADULT
    FROM {{ ref('stg_GSD__CdeEnrByRaceAndGradeEntities')}}
  ),

  final AS (
    SELECT * FROM school_enr
    UNION ALL
    SELECT * FROM entity_enr
  )

SELECT *
FROM final
ORDER BY Year, EntityCode, RaceEthnicCode, Gender

