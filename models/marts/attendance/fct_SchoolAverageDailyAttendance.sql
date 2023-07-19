with unioned as (
    select * from {{ ref('stg_SP__AverageDailyAttendance_v3') }}
    union all
    select * from {{ ref('stg_SPA__AverageDailyAttendance_v3_SY22') }}
),

final as (
    select * except (NameOfInstitution)
    from unioned
)

select * from final
