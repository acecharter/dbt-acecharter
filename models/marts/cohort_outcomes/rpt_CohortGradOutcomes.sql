WITH
  cohorts AS (
    SELECT * FROM {{ ref('fct_Cohorts')}}
  ),

  grads AS (
    SELECT * FROM {{ ref('fct_CohortOutcomes')}}
    WHERE
      Outcome = 'Regular HS Diploma'
  ),

  grad_outcomes AS (
    SELECT * FROM {{ ref('fct_CohortOutcomes')}}
    WHERE
      OutcomeType = 'Cohort Grad Outcome'
      AND Outcome != 'Regular HS Diploma'
  ),

  reporting_categories AS (
    SELECT * FROM {{ ref('stg_GSD__CdeReportingCategories')}}
  ),

  final AS (
    SELECT
      c.AcademicYear,
      CASE
        WHEN c.AggregateLevel = 'T' THEN 'State'
        WHEN c.AggregateLevel = 'C' THEN 'County'
        WHEN c.AggregateLevel = 'D' THEN 'District'
        WHEN c.AggregateLevel = 'S' THEN 'School'
      END AS EntityType,
      c.EntityCode,
      c.EntityName,
      c.CharterSchool,
      c.DASS,
      c.ReportingCategory,
      r.ReportingCategory AS ReportingCategoryName,
      r.ReportingCategoryType,
      c.CohortStudents,
      g.OutcomeCount AS RegularHsDiplomaGraduates,
      o.Outcome AS GradOutcome,
      o.OutcomeCount,
      CASE
        WHEN o.OutcomeCount = 0 OR o.OutcomeCount IS NULL THEN NULL
        ELSE ROUND(o.OutcomeCount/g.OutcomeCount, 4)
      END AS CohortGradOutcomeRate,
      CASE
        WHEN o.OutcomeCount = 0 OR o.OutcomeCount IS NULL THEN NULL
        ELSE ROUND(o.OutcomeCount/c.CohortStudents, 4) 
      END AS OverallCohortOutcomeRate      
    FROM cohorts AS c
    LEFT JOIN reporting_categories AS r
    ON c.ReportingCategory = r.ReportingCategoryCode
    LEFT JOIN grads AS g
    ON
      c.AcademicYear = g.AcademicYear
      AND c.AggregateLevel = g.AggregateLevel
      AND c.EntityCode = g.EntityCode
      AND c.CharterSchool = g.CharterSchool
      AND c.DASS = g.DASS
      AND c.ReportingCategory = g.ReportingCategory
    LEFT JOIN grad_outcomes AS o
    ON
      c.AcademicYear = o.AcademicYear
      AND c.AggregateLevel = o.AggregateLevel
      AND c.EntityCode = o.EntityCode
      AND c.CharterSchool = o.CharterSchool
      AND c.DASS = o.DASS
      AND c.ReportingCategory = o.ReportingCategory
  )

SELECT * FROM final