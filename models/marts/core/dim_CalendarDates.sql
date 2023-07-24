with final as (
    select * from {{ ref('stg_SP__CalendarDates') }}
    union all
    select * from {{ ref('stg_SPA__CalendarDates_SY23') }}
    union all
    select * from {{ ref('stg_SPA__CalendarDates_SY22') }}
)

select distinct * from final
