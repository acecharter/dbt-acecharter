WITH
  cgr AS (
    SELECT * FROM {{ ref('stg_RD__CdeCgr12Month')}}
  ),

  entity_names_ranked AS (
    SELECT
      AcademicYear,
      EntityType,
      CountyCode,
      DistrictCode,
      SchoolCode,
      CountyName,
      DistrictName,
      SchoolName,
      RANK() OVER (
        PARTITION BY
          EntityType, 
          CountyCode,
          DistrictCode, 
          SchoolCode
        ORDER BY
          AcademicYear DESC
      ) AS Rank
    FROM cgr
    WHERE
      CharterSchool = 'All'
      AND DASS = 'All'
      AND ReportingCategory = 'TA'
  ),

  state AS (
    SELECT
      c.AcademicYear,
      c.EntityType,
      '00' AS EntityCode,
      n.CountyName AS EntityName
    FROM cgr AS c
    LEFT JOIN entity_names_ranked AS n
    USING(CountyCode)
    WHERE
      c.EntityType = 'State'
      AND n.EntityType = 'State'
      AND n.Rank = 1
  ),

  county AS (
    SELECT
      c.AcademicYear,
      c.EntityType,
      c.CountyCode AS EntityCode,
      n.CountyName AS EntityName
    FROM cgr AS c
    LEFT JOIN entity_names_ranked AS n
    USING(CountyCode)
    WHERE
      c.EntityType = 'County'
      AND n.EntityType = 'County'
      AND n.Rank = 1
  ),

  district AS (
    SELECT
      c.AcademicYear,
      c.EntityType,
      c.DistrictCode AS EntityCode,
      n.DistrictName AS EntityName
    FROM cgr AS c
    LEFT JOIN entity_names_ranked AS n
    USING(DistrictCode)
    WHERE
      c.EntityType = 'District'
      AND n.EntityType = 'District'
      AND n.Rank = 1
  ),

  school AS (
    SELECT
      c.AcademicYear,
      c.EntityType,
      c.SchoolCode AS EntityCode,
      n.SchoolName AS EntityName
    FROM cgr AS c
    LEFT JOIN entity_names_ranked AS n
    USING(SchoolCode)
    WHERE
      c.EntityType = 'School'
      AND n.EntityType = 'School'
      AND n.Rank = 1
  ),

  unioned AS (
    SELECT * FROM state
    UNION ALL
    SELECT * FROM county
    UNION ALL
    SELECT * FROM district
    UNION ALL
    SELECT * FROM school
  )
  
SELECT DISTINCT
  EntityType,
  EntityCode,
  EntityName
FROM unioned
ORDER BY 1, 2
