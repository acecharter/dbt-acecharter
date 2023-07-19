with enrollment as (
    select * from {{ ref('fct_CurrentSchoolDemographicGradeEnrollment') }}
),

schools as (
    select
        SchoolYear,
        SchoolId,
        SchoolName,
        SchoolNameMid,
        SchoolNameShort
    from {{ ref('dim_CurrentSchools') }}
),

final as (
    select
        s.* except (SchoolId),
        e.* except (SchoolYear, SchoolId)
    from enrollment as e
    left join schools as s
        on
            s.SchoolYear = e.SchoolYear
            and s.SchoolId = e.SchoolId
)

select * from final
