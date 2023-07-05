with empower as (
    select * from {{ source('RawData', 'CersEmpower2021') }}
),

esperanza as (
    select * from {{ source('RawData', 'CersEsperanza2021') }}
),

inspire as (
    select * from {{ source('RawData', 'CersInspire2021') }}
),

hs as (
    select * from {{ source('RawData', 'CersHighSchool2021') }}
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
