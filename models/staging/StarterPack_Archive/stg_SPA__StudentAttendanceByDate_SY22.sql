SELECT
    '2021-22' AS SchoolYear,
    SchoolId,
    NameOfInstitution,
    StudentUniqueId,
    LastSurname,
    FirstName,
    EventDate,
    CASE WHEN AttendanceEventCategoryDescriptor = 'Unreconciled' THEN 'Absent' ELSE NULL END AS AttendanceEventCategoryDescriptor,
    EventDuration
FROM {{ source('StarterPack_Archive', 'StudentAttendanceByDate_SY22')}}
WHERE StudentUniqueId != '16348' --This is a fake/test student in PS