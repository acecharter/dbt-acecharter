WITH
  cgr AS (
    SELECT * FROM {{ ref('fct_CollegeGoingRate')}}
  ),

  comparison_entities AS (
    SELECT * FROM {{ ref('stg_GSD__ComparisonEntities')}}
    WHERE AceComparisonSchoolCode = '0125617'
  ),

  reporting_categories AS (
    SELECT * FROM {{ ref('stg_GSD__CdeReportingCategories')}}
    
  ),

  final AS (
    SELECT
      c.AcademicYear,
      e.EntityType,
      c.EntityCode,
      e.EntityName,
      e.EntityNameShort,
      c.CharterSchool,
      c.DASS,
      c.ReportingCategory,
      r.ReportingCategory AS ReportingCategoryName,
      r.ReportingCategoryType,
      c.CompleterType,
      CASE
        WHEN c.CompleterType = 'AGY' THEN 'Graduates meeting a-g requirements'
        WHEN c.CompleterType = 'AGN' THEN 'Graduates not meeting a-g requirements'
        WHEN c.CompleterType = 'NGC' THEN 'Non-graduate completers not meeting a-g requirements'
        WHEN c.CompleterType = 'TA' THEN 'Total (all HS completers)'
      END AS CompleterTypeDescription,
      c.CgrPeriodType,
      c.HighSchoolCompleters,
      c.EnrolledInCollegeTotal,
      c.CollegeGoingRateTotal,
      c.EnrolledInState,
      c.EnrolledOutOfState,
      c.NotEnrolledInCollege,
      c.EnrolledUc,
      c.EnrolledCsu,
      c.EnrolledCcc,
      c.EnrolledInStatePrivate2And4Year,
      c.EnrolledOutOfState4YearCollegePublicPrivate,
      c.EnrolledOutOfState2YearCollegePublicPrivate
    FROM cgr AS c
    LEFT JOIN comparison_entities AS e
    ON c.EntityCode = e.EntityCode
    LEFT JOIN reporting_categories AS r
    ON c.ReportingCategory = r.ReportingCategoryCode
  )

SELECT * FROM final