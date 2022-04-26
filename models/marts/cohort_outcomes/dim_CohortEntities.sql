WITH
  cohort_outcomes AS (
    SELECT * FROM {{ ref('stg_RD__CdeAdjustedCohortOutcomes')}}
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
    FROM cohort_outcomes
    WHERE
      CharterSchool = 'All'
      AND DASS = 'All'
      AND ReportingCategory = 'TA'
  ),

  state AS (
    SELECT
      o.AcademicYear,
      o.EntityType,
      '00' AS EntityCode,
      n.CountyName AS EntityName
    FROM cohort_outcomes AS o
    LEFT JOIN entity_names_ranked AS n
    USING(CountyCode)
    WHERE
      o.EntityType = 'State'
      AND n.EntityType = 'State'
      AND n.Rank = 1
  ),

  county AS (
    SELECT
      o.AcademicYear,
      o.EntityType,
      o.CountyCode AS EntityCode,
      n.CountyName AS EntityName
    FROM cohort_outcomes AS o
    LEFT JOIN entity_names_ranked AS n
    USING(CountyCode)
    WHERE
      o.EntityType = 'County'
      AND n.EntityType = 'County'
      AND n.Rank = 1
  ),

  district AS (
    SELECT
      o.AcademicYear,
      o.EntityType,
      o.DistrictCode AS EntityCode,
      n.DistrictName AS EntityName
    FROM cohort_outcomes AS o
    LEFT JOIN entity_names_ranked AS n
    USING(DistrictCode)
    WHERE
      o.EntityType = 'District'
      AND n.EntityType = 'District'
      AND n.Rank = 1
  ),

  school AS (
    SELECT
      o.AcademicYear,
      o.EntityType,
      o.SchoolCode AS EntityCode,
      n.SchoolName AS EntityName
    FROM cohort_outcomes AS o
    LEFT JOIN entity_names_ranked AS n
    USING(SchoolCode)
    WHERE
      o.EntityType = 'School'
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
