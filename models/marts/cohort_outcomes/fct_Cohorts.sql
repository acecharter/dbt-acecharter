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

  cohort_state AS (
    SELECT
      AcademicYear,
      EntityType,
      '0' AS EntityCode,
      'State' AS EntityName,
      CharterSchool,
      DASS,
      ReportingCategory,
      CohortStudents
    FROM cohort_outcomes
    WHERE EntityType = 'State'
  ),

  cohort_county AS (
    SELECT
      o.AcademicYear,
      o.EntityType,
      o.CountyCode AS EntityCode,
      n.CountyName AS EntityName,
      o.CharterSchool,
      o.DASS,
      o.ReportingCategory,
      o.CohortStudents
    FROM cohort_outcomes AS o
    LEFT JOIN entity_names_ranked AS n
    USING(CountyCode)
    WHERE
      o.EntityType = 'County'
      AND n.EntityType = 'County'
      AND n.Rank = 1
  ),

  cohort_district AS (
    SELECT
      o.AcademicYear,
      o.EntityType,
      o.DistrictCode AS EntityCode,
      n.DistrictName AS EntityName,
      o.CharterSchool,
      o.DASS,
      o.ReportingCategory,
      o.CohortStudents
    FROM cohort_outcomes AS o
    LEFT JOIN entity_names_ranked AS n
    USING(DistrictCode)
    WHERE
      o.EntityType = 'District'
      AND n.EntityType = 'District'
      AND n.Rank = 1
  ),

  cohort_school AS (
    SELECT
      o.AcademicYear,
      o.EntityType,
      o.SchoolCode AS EntityCode,
      n.SchoolName AS EntityName,
      o.CharterSchool,
      o.DASS,
      o.ReportingCategory,
      o.CohortStudents
    FROM cohort_outcomes AS o
    LEFT JOIN entity_names_ranked AS n
    USING(SchoolCode)
    WHERE
      o.EntityType = 'School'
      AND n.EntityType = 'School'
      AND n.Rank = 1
  ),

  unioned AS (
    SELECT * FROM cohort_state
    UNION ALL
    SELECT * FROM cohort_county
    UNION ALL
    SELECT * FROM cohort_district
    UNION ALL
    SELECT * FROM cohort_school
  )
  
SELECT * FROM unioned
WHERE CohortStudents IS NOT NULL
