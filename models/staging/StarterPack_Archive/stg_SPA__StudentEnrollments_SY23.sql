select
    '2022-23' as SchoolYear,
    SchoolId,
    NameOfInstitution as SchoolName,
    StudentUniqueId,
    LastSurname as LastName,
    FirstName,
    cast(GradeLevel as int64) as GradeLevel,
    EntryDate,
    ExitWithdrawDate,
    ExitWithdrawReason,
    false as IsCurrentEnrollment
from {{ source('StarterPack_Archive', 'StudentEnrollments_SY23') }}
where StudentUniqueId not in ('16671', '16667', '16668') -- These are fake/test student accounts
