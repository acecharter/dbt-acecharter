SELECT
  StudentRenaissanceID,
  CAST(StudentIdentifier AS STRING) AS StudentIdentifier,
  CAST(StateUniqueId AS STRING) AS StateUniqueId
FROM {{ source('RenaissanceStar_Archive', 'MissingStudentIds')}}