with unioned as (
    select * from {{ ref('stg_SP__StudentAttendanceByDate') }}
    union all
    select * from {{ ref('stg_SPA__StudentAttendanceByDate_SY22') }}
),

final as (
    select *
    from unioned
    where AttendanceEventCategoryDescriptor = 'Absent'
)

select * from final
