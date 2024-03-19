with inspire as (
    select * from {{ source('RawData', 'CersInspire2324') }}
)

select distinct * from inspire
