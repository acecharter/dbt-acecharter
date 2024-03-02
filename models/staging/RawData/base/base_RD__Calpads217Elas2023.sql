with empower as (
    select * from {{ ref('base_RD__Calpads217Elas2023Empower')}}
),

esperanza as (
    select * from {{ ref('base_RD__Calpads217Elas2023Esperanza')}}
),

hs as (
    select * from {{ ref('base_RD__Calpads217Elas2023HighSchool')}}
),

inspire as (
    select * from {{ ref('base_RD__Calpads217Elas2023Inspire')}}
),

unioned as (
    select * from empower
    union all
    select * from esperanza
    union all
    select * from hs
    union all
    select * from inspire
),

final as (
    select
        '2022-23' as SchoolYear,
        *
    from unioned
)

select * from final
