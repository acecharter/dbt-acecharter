with empower as (
    select * from {{ ref('base_RD__Calpads217Elas2022Empower')}}
),

esperanza as (
    select * from {{ ref('base_RD__Calpads217Elas2022Esperanza')}}
),

hs as (
    select * from {{ ref('base_RD__Calpads217Elas2022HighSchool')}}
),

inspire as (
    select * from {{ ref('base_RD__Calpads217Elas2022Inspire')}}
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
        '2021-22' as SchoolYear,
        *
    from unioned
)

select * from final
