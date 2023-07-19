select
    '2021-22' as SchoolYear,
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
from {{ source('StarterPack_Archive', 'StudentEnrollments_SY22') }}
where StudentUniqueId != '16348' --This is a fake/test student
