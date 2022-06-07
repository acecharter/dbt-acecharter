WITH
  cgr AS (
    SELECT * FROM {{ ref('stg_RD__CdeCgr12Month')}}
  ),

  entity_names AS (
    SELECT
      EntityType,
      CountyCode,
      DistrictCode,
      SchoolCode,
      CountyName,
      DistrictName,
      SchoolName,
      MAX(AcademicYear) AS MaxAcademicYear
    FROM cgr

    GROUP BY 1, 2, 3, 4, 5, 6, 7
  ),

  state AS (
    SELECT
      EntityType,
      '00' AS EntityCode,
      CountyName AS EntityName
    FROM entity_names
    WHERE EntityType = 'State'
  ),

  county AS (
    SELECT
      EntityType,
      CountyCode AS EntityCode,
      CountyName AS EntityName
    FROM entity_names
    WHERE EntityType = 'County'
  ),

  district AS (
    SELECT
      EntityType,
      DistrictCode AS EntityCode,
      DistrictName AS EntityName
    FROM entity_names
    WHERE EntityType = 'District'
  ),

  school AS (
    SELECT
      EntityType,
      SchoolCode AS EntityCode,
      SchoolName AS EntityName
    FROM entity_names
    WHERE EntityType = 'School'
  ),

  unioned AS (
    SELECT * FROM state
    UNION ALL
    SELECT * FROM county
    UNION ALL
    SELECT * FROM district
    UNION ALL
    SELECT * FROM school
  ),
  
  final AS (
    SELECT * FROM unioned
  )

SELECT *
FROM final
ORDER BY 1, 2
