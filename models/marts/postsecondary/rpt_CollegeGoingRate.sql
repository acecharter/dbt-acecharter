with cgr as (
    select * from {{ ref('fct_CollegeGoingRate') }}
),

entities as (
    select * from {{ ref('dim_CgrEntities') }}
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
        c.AcademicYear,
        e.EntityType,
        c.EntityCode,
        e.EntityName,
        c.CharterSchool,
        c.DASS,
        c.ReportingCategory,
        r.ReportingCategory as ReportingCategoryName,
        r.ReportingCategoryType,
        c.CompleterType,
        case
            when
                c.CompleterType = 'AGY'
                then 'Graduates meeting a-g requirements'
            when
                c.CompleterType = 'AGN'
                then 'Graduates not meeting a-g requirements'
            when
                c.CompleterType = 'NGC'
                then 'Non-graduate completers not meeting a-g requirements'
            when c.CompleterType = 'TA' then 'Total (all HS completers)'
        end as CompleterTypeDescription,
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
    from cgr as c
    left join entities as e
        on c.EntityCode = e.EntityCode
    left join reporting_categories as r
        on c.ReportingCategory = r.ReportingCategoryCode
    where
        c.EntityCode = '0125617'
        or c.EntityCode in (select EntityCode from comparison_entities)
)

select * from final
