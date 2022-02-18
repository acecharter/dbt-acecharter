-- No fields dropped from source table

SELECT
  SchoolId,
  NameOfInstitution AS SchoolName,
  StudentUniqueId,
  LastSurname,
  FirstName,
  EventDate,
  CASE WHEN AttendanceEventCategoryDescriptor = 'Unreconciled' THEN 'Absent' ELSE 'Other' END AS AttendanceEventCategoryDescriptor,
  EventDuration
FROM {{ source('StarterPack', 'StudentAttendanceByDate')}}