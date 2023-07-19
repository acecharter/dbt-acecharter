with source_table as (
    select * from {{ source('StarterPack', 'Schools') }}
),

school_year as (
    select distinct SchoolYear
    from {{ ref('stg_SP__CalendarDates') }}
),

final as (
    select
        school_year.SchoolYear,
        source_table.*
    from source_table
    cross join school_year
)

select * from final
