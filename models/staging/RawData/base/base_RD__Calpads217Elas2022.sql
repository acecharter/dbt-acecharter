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

final as (
    select * from empower
    union all
    select * from esperanza
    union all
    select * from hs
    union all
    select * from inspire
)

select * from final
