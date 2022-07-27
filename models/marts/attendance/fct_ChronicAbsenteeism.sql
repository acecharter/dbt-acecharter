WITH
  entities AS (
    SELECT * FROM {{ ref('dim_Entities')}}
  ),

  chrabs AS (
    SELECT
      *,
       CASE
        WHEN EntityType = 'State' THEN '00'
        WHEN EntityType = 'County' THEN CountyCode
        WHEN EntityType = 'District' THEN DistrictCode
        WHEN EntityType = 'School' THEN SchoolCode
       END AS EntityCode,     
    FROM {{ ref('stg_RD__CdeChronicAbsenteeism')}}
  ),

  reporting_categories AS (
    SELECT * FROM {{ ref('stg_GSD__CdeReportingCategories')}}
  ),

  final AS (
    SELECT
      c.AcademicYear,
      e.EntityType,
      c.EntityCode,
      e.EntityNameShort AS EntityName,
      c.CharterSchool,
      c.ReportingCategory,
      r.ReportingCategory AS ReportingCategoryName,
      r.ReportingCategoryType,
      c.ChronicAbsenteeismEligibleCumula,
      c.ChronicAbsenteeismCount,
      c.ChronicAbsenteeismRate
    FROM chrabs AS c 
    LEFT JOIN entities AS e
    ON c.EntityCode = e.EntityCode
    LEFT JOIN reporting_categories AS r
    ON c.ReportingCategory = r.ReportingCategoryCode
    WHERE
      e.EntityCode IS NOT NULL
      AND c.ChronicAbsenteeismEligibleCumula IS NOT NULL
  )

SELECT * FROM final
ORDER BY 1, 3, 5, 6