with schools as (
    select
        SchoolYear,
        SchoolId,
        SchoolName,
        SchoolNameMid,
        SchoolNameShort
    from {{ ref('dim_Schools') }}
),

students as (
    select
        * except (
            LastName,
            FirstName,
            MiddleName,
            BirthDate
        )
    from {{ ref('dim_Students') }}
),

attendance as (
    select * from {{ ref('fct_StudentAttendance') }}
),

final as (
    select
        a.SchoolYear,
        sc.* except (SchoolYear),
        st.* except (SchoolYear, SchoolId),
        a.* except (SchoolId, StudentUniqueId, SchoolYear)
    from attendance as a
    left join schools as sc
        on
            a.SchoolId = sc.SchoolId
            and a.SchoolYear = sc.SchoolYear
    left join students as st
        on
            a.SchoolId = st.SchoolId
            and a.StudentUniqueId = st.StudentUniqueId
            and a.SchoolYear = st.SchoolYear
)

select * from final
