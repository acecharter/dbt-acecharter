with cgr as (
    select * from {{ ref('int_CdeCgr__unioned') }}
),

entity_names as (
    select
        EntityType,
        CountyCode,
        DistrictCode,
        SchoolCode,
        CountyName,
        DistrictName,
        SchoolName,
        max(AcademicYear) as MaxAcademicYear
    from cgr
    group by 1, 2, 3, 4, 5, 6, 7
),

final as (
    select
        EntityType,
        case
            when EntityType = 'State' then '00'
            when EntityType = 'County' then CountyCode
            when EntityType = 'District' then DistrictCode
            when EntityType = 'School' then SchoolCode
        end as EntityCode,
        case
            when EntityType in ('State', 'County') then CountyName
            when EntityType = 'District' then DistrictName
            when EntityType = 'School' then SchoolName
        end as EntityName
    from entity_names
)

select *
from final
order by 1, 2
