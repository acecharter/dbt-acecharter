WITH
  outcomes AS (
    SELECT * FROM {{ ref('fct_CohortOutcomes')}}
  ),

  entities AS (
    SELECT * FROM {{ ref('dim_CohortEntities')}}
  ),

  reporting_categories AS (
    SELECT * FROM {{ ref('stg_GSD__CdeReportingCategories')}}
  ),

  comparison_entities AS (
    SELECT * FROM {{ ref('stg_GSD__ComparisonEntities')}}
    WHERE AceComparisonSchoolCode = '0125617'
  ),

  final AS (
    SELECT
      o.AcademicYear,
      e.EntityType,
      o.EntityCode,
      e.EntityName,
      o.CharterSchool,
      o.DASS,
      o.ReportingCategory,
      r.ReportingCategory AS ReportingCategoryName,
      r.ReportingCategoryType,
      o.OutcomeType,
      o.OutcomeDenominator,
      o.Outcome,
      o.OutcomeCount,
      o.OutcomeRate
    FROM outcomes AS o
    LEFT JOIN entities AS e
    ON o.EntityCode = e.EntityCode
    LEFT JOIN reporting_categories AS r
    ON o.ReportingCategory = r.ReportingCategoryCode
    WHERE
      o.EntityCode = '0125617' OR
      o.EntityCode IN (SELECT EntityCode FROM comparison_entities)
  )

SELECT * FROM final