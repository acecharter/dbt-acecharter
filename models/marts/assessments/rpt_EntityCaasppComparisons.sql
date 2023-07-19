with entities as (
    select * from {{ ref('dim_ComparisonEntities') }}
),

caaspp as (
    select * from {{ ref('fct_EntityCaaspp') }}
),

empower as (
    select
        entities.AceComparisonSchoolName,
        entities.AceComparisonSchoolCode,
        caaspp.*
    from entities
    left join caaspp
        on entities.EntityCode = caaspp.EntityCode
    where
        entities.AceComparisonSchoolCode = '0116814'
        and GradeLevel in (5, 6, 7, 8, 13)
),

esperanza as (
    select
        entities.AceComparisonSchoolName,
        entities.AceComparisonSchoolCode,
        caaspp.*
    from entities
    left join caaspp
        on entities.EntityCode = caaspp.EntityCode
    where
        entities.AceComparisonSchoolCode = '0129247'
        and GradeLevel in (5, 6, 7, 8, 13)
),

inspire as (
    select
        entities.AceComparisonSchoolName,
        entities.AceComparisonSchoolCode,
        caaspp.*
    from entities
    left join caaspp
        on entities.EntityCode = caaspp.EntityCode
    where
        entities.AceComparisonSchoolCode = '0131656'
        and GradeLevel in (5, 6, 7, 8, 13)
),

ace_hs as (
    select
        entities.AceComparisonSchoolName,
        entities.AceComparisonSchoolCode,
        caaspp.*
    from entities
    left join caaspp
        on entities.EntityCode = caaspp.EntityCode
    where
        entities.AceComparisonSchoolCode = '0125617'
        and GradeLevel in (11, 13)
),

final as (
    select * from empower
    union all
    select * from esperanza
    union all
    select * from inspire
    union all
    select * from ace_hs
)

select * from final
