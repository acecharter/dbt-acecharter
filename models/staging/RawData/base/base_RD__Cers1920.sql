with empower as (
    select * from {{ source('RawData', 'CersEmpower1920') }}
),

esperanza as (
    select * from {{ source('RawData', 'CersEsperanza1920') }}
),

inspire as (
    select * from {{ source('RawData', 'CersInspire1920') }}
),

hs as (
    select * from {{ source('RawData', 'CersHighSchool1920') }}
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
