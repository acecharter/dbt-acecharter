WITH
  cohort_outcomes AS (
    SELECT * FROM {{ ref('stg_RD__CdeAdjustedCohortOutcomes')}}
  ),

  entity_names_ranked AS (
    SELECT
      AggregateLevel,
      CountyCode,
      DistrictCode,
      SchoolCode,
      CountyName,
      DistrictName,
      SchoolName,
      RANK() OVER (
        PARTITION BY
          AggregateLevel, 
          CountyCode,
          DistrictCode, 
          SchoolCode,
          CountyName, 
          DistrictName, 
          SchoolName
        ORDER BY 
          AggregateLevel, 
          CountyCode,
          DistrictCode, 
          SchoolCode,
          CountyName, 
          DistrictName, 
          SchoolName,
          AcademicYear DESC
      ) AS Rank
    FROM cohort_outcomes
  ),

  cohort_state AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      '0' AS EntityCode,
      'State of California' AS EntityName,
      CharterSchool,
      DASS,
      ReportingCategory,
      CohortStudents
    FROM cohort_outcomes
    WHERE AggregateLevel = 'T'
  ),

  cohort_county AS (
    SELECT
      o.AcademicYear,
      o.AggregateLevel,
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
      o.AggregateLevel = 'C'
      AND n.Rank = 1
  ),

  cohort_district AS (
    SELECT
      o.AcademicYear,
      o.AggregateLevel,
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
      o.AggregateLevel = 'D'
      AND n.Rank = 1
  ),

  cohort_school AS (
    SELECT
      o.AcademicYear,
      o.AggregateLevel,
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
      o.AggregateLevel = 'S'
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
