with empower as (
    select * from {{ source('RawData', 'CersEmpower2324') }}
),

esperanza as (
    select * from {{ source('RawData', 'CersEsperanza2324') }}
),

inspire as (
    select * from {{ source('RawData', 'CersInspire2324') }}
),

hs as (
    select * from {{ source('RawData', 'CersHighSchool2223') }}
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
