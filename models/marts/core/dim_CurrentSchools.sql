with current_school_year as (
    select distinct SchoolYear
    from {{ ref('stg_SP__CalendarDates') }}
),

schools as (
    select * from {{ ref('dim_Schools') }}
),

final as (
    select schools.*
    from schools
    where SchoolYear = (select * from current_school_year)
)

select * from final
