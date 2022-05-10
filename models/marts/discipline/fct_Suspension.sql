WITH 
  entities AS (
    SELECT DISTINCT * EXCEPT (AceComparisonSchoolCode,AceComparisonSchoolName)
    FROM {{ ref('dim_ComparisonEntities')}}
  ),

  suspension AS (
    SELECT
      *,
       CASE
        WHEN EntityType = 'State' THEN '00'
        WHEN EntityType = 'County' THEN CountyCode
        WHEN EntityType = 'District' THEN DistrictCode
        WHEN EntityType = 'School' THEN SchoolCode
       END AS EntityCode,     
    FROM {{ ref('stg_RD__CdeSuspension')}}
  ),

  reporting_categories AS (
    SELECT * FROM {{ ref('stg_GSD__CdeReportingCategories')}}
  ),

  final AS (
    SELECT
      s.AcademicYear,
      e.EntityType,
      s.EntityCode,
      e.EntityName,
      s.CharterSchool,
      s.ReportingCategory,
      r.ReportingCategory AS ReportingCategoryName,
      r.ReportingCategoryType,
      s.CumulativeEnrollment,
      s.TotalSuspensions,
      s.UnduplicatedCountOfStudentsSuspendedTotal AS UnduplicatedCountOfStudentsSuspended,
      s.SuspensionRateTotal AS SuspensionRate
    FROM suspension AS s
    LEFT JOIN entities AS e
    ON s.EntityCode = e.EntityCode
    LEFT JOIN reporting_categories AS r
    ON s.ReportingCategory = r.ReportingCategoryCode
    WHERE
      e.EntityCode IS NOT NULL
      AND s.CumulativeEnrollment IS NOT NULL
  )

SELECT * FROM final
ORDER BY 1, 3, 5, 6