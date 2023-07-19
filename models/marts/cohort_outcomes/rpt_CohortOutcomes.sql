with outcomes as (
    select * from {{ ref('fct_CohortOutcomes') }}
),

entities as (
    select * from {{ ref('dim_CohortEntities') }}
),

reporting_categories as (
    select * from {{ ref('stg_GSD__CdeReportingCategories') }}
),

comparison_entities as (
    select *
    from {{ ref('stg_GSD__ComparisonEntities') }}
    where AceComparisonSchoolCode = '0125617'
),

final as (
    select
        o.AcademicYear,
        e.EntityType,
        o.EntityCode,
        e.EntityName,
        o.CharterSchool,
        o.DASS,
        o.ReportingCategory,
        r.ReportingCategory as ReportingCategoryName,
        r.ReportingCategoryType,
        o.OutcomeType,
        o.OutcomeDenominator,
        o.Outcome,
        o.OutcomeCount,
        o.OutcomeRate
    from outcomes as o
    left join entities as e
        on o.EntityCode = e.EntityCode
    left join reporting_categories as r
        on o.ReportingCategory = r.ReportingCategoryCode
    where
        o.EntityCode = '0125617'
        or o.EntityCode in (select EntityCode from comparison_entities)
)

select * from final
