with empower as (
    select * from {{ source('RawData', 'CersEmpower1819') }}
),

esperanza as (
    select * from {{ source('RawData', 'CersEsperanza1819') }}
),

inspire as (
    select * from {{ source('RawData', 'CersInspire1819') }}
),

hs as (
    select * from {{ source('RawData', 'CersHighSchool1819') }}
),

final as (
    select * from empower
    union all
    select * from esperanza
    union all
    select * from inspire
    union all
    select * from hs
)

select distinct * from final
