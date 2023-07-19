with entities as (
    select * from {{ ref('dim_Entities') }}
),

chrabs as (
    select
        *,
        case
            when EntityType = 'State' then '00'
            when EntityType = 'County' then CountyCode
            when EntityType = 'District' then DistrictCode
            when EntityType = 'School' then SchoolCode
        end as EntityCode
    from {{ ref('stg_RD__CdeChronicAbsenteeism') }}
),

reporting_categories as (
    select * from {{ ref('stg_GSD__CdeReportingCategories') }}
),

final as (
    select
        c.AcademicYear,
        e.EntityType,
        c.EntityCode,
        e.EntityNameShort as EntityName,
        c.CharterSchool,
        c.ReportingCategory,
        r.ReportingCategory as ReportingCategoryName,
        r.ReportingCategoryType,
        c.ChronicAbsenteeismEligibleCumula,
        c.ChronicAbsenteeismCount,
        c.ChronicAbsenteeismRate
    from chrabs as c
    left join entities as e
        on c.EntityCode = e.EntityCode
    left join reporting_categories as r
        on c.ReportingCategory = r.ReportingCategoryCode
    where
        e.EntityCode is not null
        and c.ChronicAbsenteeismEligibleCumula is not null
)

select * from final
order by 1, 3, 5, 6
