with
ace_entities as (
    select
        StateSchoolCode as EntityCode,
        'School' as EntityType,
        SchoolNameFull as EntityName,
        SchoolNameMid as EntityNameMid,
        SchoolNameShort as EntityNameShort,
        StateSchoolCode as AceComparisonSchoolCode
    from {{ ref('stg_GSD__Schools') }}
),

comparison_entities as (
    select
        EntityCode,
        EntityType,
        EntityName,
        EntityNameShort as EntityNameMid,
        EntityNameShort,
        AceComparisonSchoolCode
    from {{ ref('stg_GSD__ComparisonEntities') }}
),

unioned as (
    select * from ace_entities
    union all
    select * from comparison_entities
),

final as (
    select
        u.*,
        a.EntityNameMid as AceComparisonSchoolName
    from unioned as u
    left join ace_entities as a
        on u.AceComparisonSchoolCode = a.EntityCode
)

select * from final
