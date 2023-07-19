with cohort_outcomes as (
    select * from {{ ref('stg_RD__CdeAdjustedCohortOutcomes') }}
),

entity_names_ranked as (
    select
        AcademicYear,
        EntityType,
        CountyCode,
        DistrictCode,
        SchoolCode,
        CountyName,
        DistrictName,
        SchoolName,
        rank() over (
            partition by
                EntityType,
                CountyCode,
                DistrictCode,
                SchoolCode
            order by
                AcademicYear desc
        ) as Rank
    from cohort_outcomes
    where
        CharterSchool = 'All'
        and DASS = 'All'
        and ReportingCategory = 'TA'
),

state as (
    select
        o.AcademicYear,
        o.EntityType,
        '00' as EntityCode,
        n.CountyName as EntityName
    from cohort_outcomes as o
    left join entity_names_ranked as n
        on o.CountyCode = n.CountyCode
where
    o.EntityType = 'State'
    and n.EntityType = 'State'
    and n.Rank = 1
),

county as (
select
    o.AcademicYear,
    o.EntityType,
    o.CountyCode as EntityCode,
    n.CountyName as EntityName
from cohort_outcomes as o
left join entity_names_ranked as n
    on o.CountyCode = n.CountyCode
where
    o.EntityType = 'County'
    and n.EntityType = 'County'
    and n.Rank = 1
),

district as (
    select
        o.AcademicYear,
        o.EntityType,
        o.DistrictCode as EntityCode,
        n.DistrictName as EntityName
    from cohort_outcomes as o
    left join entity_names_ranked as n
        on o.DistrictCode = n.DistrictCode
    where
        o.EntityType = 'District'
        and n.EntityType = 'District'
        and n.Rank = 1
),

school as (
    select
        o.AcademicYear,
        o.EntityType,
        o.SchoolCode as EntityCode,
        n.SchoolName as EntityName
    from cohort_outcomes as o
    left join entity_names_ranked as n
        on o.SchoolCode = n.SchoolCode
    where
        o.EntityType = 'School'
        and n.EntityType = 'School'
        and n.Rank = 1
),

unioned as (
    select * from state
    union all
    select * from county
    union all
    select * from district
    union all
    select * from school
)

select distinct
    EntityType,
    EntityCode,
    EntityName
from unioned
order by 1, 2
