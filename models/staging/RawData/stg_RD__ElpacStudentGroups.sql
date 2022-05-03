SELECT
  CAST(Student_Group_ID AS STRING) AS StudentGroupId,
  Student_Group_Name AS StudentGroupName
FROM {{ source('RawData', 'ElpacStudentGroups')}}