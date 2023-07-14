with empower as (
    select * from {{ source('RawData', 'CersEmpower2122') }}
),

esperanza as (
    select * from {{ source('RawData', 'CersEsperanza2122') }}
),

inspire as (
    select * from {{ source('RawData', 'CersInspire2122') }}
),

hs as (
    select * from {{ source('RawData', 'CersHighSchool2122') }}
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
