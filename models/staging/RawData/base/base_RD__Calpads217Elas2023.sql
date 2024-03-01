with empower as (
    select * from {{ ref('base_RD__Calpads217ElasEmpower2023')}}
),

esperanza as (
    select * from {{ ref('base_RD__Calpads217ElasEsperanza2023')}}
),

hs as (
    select * from {{ ref('base_RD__Calpads217ElasHighSchool2023')}}
),

inspire as (
    select * from {{ ref('base_RD__Calpads217ElasInspire2023')}}
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
