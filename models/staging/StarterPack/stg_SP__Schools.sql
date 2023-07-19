with source_table as (
    select * from {{ source('StarterPack', 'Schools') }}
),

school_year as (
    select distinct SchoolYear
    from {{ ref('stg_SP__CalendarDates') }}
),

final as (
    select
        sy.SchoolYear,
        source_table.*
    from source_table
    cross join sy
)

select * from final
