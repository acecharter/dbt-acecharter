with empower as (
    select * from {{ source('RawData', 'CersEmpower2223') }}
),

esperanza as (
    select * from {{ source('RawData', 'CersEsperanza2223') }}
),

inspire as (
    select * from {{ source('RawData', 'CersInspire2223') }}
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
