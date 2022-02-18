SELECT
  SchoolId,
  NameOfInstitution AS SchoolName,
  StudentUniqueId,
  LastSurname,
  FirstName,
  EventDate,
  CASE WHEN AttendanceEventCategoryDescriptor = 'Unreconciled' THEN 'Absent' ELSE NULL END AS AttendanceEventCategoryDescriptor,
  EventDuration
FROM {{ source('StarterPack', 'StudentAttendanceByDate')}}