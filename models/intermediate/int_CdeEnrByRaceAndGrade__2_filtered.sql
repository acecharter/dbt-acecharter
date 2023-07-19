with
entities as (
    select * from {{ ref('dim_Entities') }}
),

enrollment as (
    select * from {{ ref('int_CdeEnrByRaceAndGrade__1_unioned') }}
),

final as (
    select enr.*
    from enrollment as enr
    left join entities
        on enr.EntityCode = entities.EntityCode
    where entities.EntityCode is not null
)

select * from final
