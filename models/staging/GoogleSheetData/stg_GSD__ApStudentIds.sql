select
    ApId,
    StudentIdentifier,
    LastName,
    FirstName,
    MiddleInitial,
    DateOfBirth,
    cast(StateUniqueId as string) as StateUniqueId,
    cast(StudentUniqueId as string) as StudentUniqueId
from {{ source('GoogleSheetData', 'ApStudentIds') }}
