with empower as (
    select * from {{ ref('base_RD__Calpads217SelaEmpower2022')}}
),

esperanza as (
    select * from {{ ref('base_RD__Calpads217SelaEsperanza2022')}}
),

hs as (
    select * from {{ ref('base_RD__Calpads217SelaHighSchool2022')}}
),

inspire as (
    select * from {{ ref('base_RD__Calpads217SelaInspire2022')}}
)

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
