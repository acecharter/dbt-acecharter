WITH
  enr AS (
    SELECT * FROM {{ ref('stg_RD__CdeEnr') }}
  ),

  school_max_year AS (
    SELECT DISTINCT
      SchoolCode,
      MAX(Year) OVER (PARTITION BY SchoolCode) AS MaxYear
    FROM enr
  ),

  max_year_school_names AS (
    SELECT DISTINCT
      m.SchoolCode,
      e.School
    FROM school_max_year AS m
    LEFT JOIN enr AS e
    USING(SchoolCode)
    WHERE m.MaxYear = e.Year      
  )

SELECT * FROM max_year_school_names


