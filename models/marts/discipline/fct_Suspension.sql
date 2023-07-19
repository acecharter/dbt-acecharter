with entities as (
    select * from {{ ref('dim_Entities') }}
),

suspension as (
    select
        *,
        case
            when EntityType = 'State' then '00'
            when EntityType = 'County' then CountyCode
            when EntityType = 'District' then DistrictCode
            when EntityType = 'School' then SchoolCode
        end as EntityCode
    from {{ ref('stg_RD__CdeSuspension') }}
),

reporting_categories as (
    select * from {{ ref('stg_GSD__CdeReportingCategories') }}
),

final as (
    select
        s.AcademicYear,
        e.EntityType,
        s.EntityCode,
        e.EntityNameShort as EntityName,
        s.CharterSchool,
        s.ReportingCategory,
        r.ReportingCategory as ReportingCategoryName,
        r.ReportingCategoryType,
        s.CumulativeEnrollment,
        s.TotalSuspensions,
        s.UnduplicatedCountOfStudentsSuspendedTotal
            as UnduplicatedCountOfStudentsSuspended,
        s.SuspensionRateTotal as SuspensionRate
    from suspension as s
    left join entities as e
        on s.EntityCode = e.EntityCode
    left join reporting_categories as r
        on s.ReportingCategory = r.ReportingCategoryCode
    where
        e.EntityCode is not null
        and s.CumulativeEnrollment is not null
)

select * from final
order by 1, 3, 5, 6
