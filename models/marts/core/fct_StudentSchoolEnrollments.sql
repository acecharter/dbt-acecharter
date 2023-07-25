with unioned as (
    select * from {{ ref('stg_SP__StudentEnrollments') }}
    union all
    select * from {{ ref('stg_SPA__StudentEnrollments_SY23') }}
    union all
    select * from {{ ref('stg_SPA__StudentEnrollments_SY22') }}
),

final as (
    select
        SchoolYear,
        SchoolId,
        StudentUniqueId,
        GradeLevel,
        EntryDate,
        ExitWithdrawDate,
        ExitWithdrawReason,
        IsCurrentEnrollment
    from unioned
)

select distinct * from final
