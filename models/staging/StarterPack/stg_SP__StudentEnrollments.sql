with source_table as (
    select
        SchoolId,
        NameOfInstitution as SchoolName,
        StudentUniqueId,
        LastSurname as LastName,
        FirstName,
        cast(GradeLevel as int64) as GradeLevel,
        EntryDate,
        ExitWithdrawDate,
        ExitWithdrawReason,
        IsCurrentEnrollment
    from {{ source('StarterPack', 'StudentEnrollments') }}
    -- These are fake/test student accounts
    where StudentUniqueId not in ('16671', '16667', '16668')
),

sy as (
    select * from {{ ref('dim_CurrentSchoolYear') }}
),

final as (
    select
        sy.SchoolYear,
        source_table.*
    from source_table
    cross join sy
)

select * from final
