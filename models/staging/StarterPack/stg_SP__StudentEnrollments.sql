select
    case
        when extract(month from EntryDate) > 7
            then concat(
                extract(year from EntryDate),
                '-',
                substr(cast((extract(year from EntryDate) + 1) as string), 3, 2)
            )
        when extract(month from EntryDate) <= 7
            then concat(
                extract(year from EntryDate) - 1,
                '-',
                extract(year from EntryDate) - 2000
            )
        else 'ERROR'
    end as SchoolYear,
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
where StudentUniqueId not in ('16671', '16667', '16668') -- These are fake/test student accounts
