SELECT
  SchoolId,
  NameOfInstitution AS SchoolName,
  CASE
    WHEN SchoolId = '116814' THEN 'Empower'
    WHEN SchoolId = '131656' THEN 'Inspire'
    WHEN SchoolId = '129247' THEN 'Esperanza'
    WHEN SchoolId = '125617' THEN 'ACE High'
  END AS SchoolNameShort,
  CASE
    WHEN SchoolId = '116814' THEN 'Middle'
    WHEN SchoolId = '131656' THEN 'Middle'
    WHEN SchoolId = '129247' THEN 'Middle'
    WHEN SchoolId = '125617' THEN 'High'
  END AS SchoolType,
  PhysicalStreetNumberName,
  PhysicalCity,
  PhysicalStateAbbreviation,
  PhysicalPostalCode,
  MailingStreetNumberName,
  MailingCity,
  MailingStateAbbreviation,
  MailingPostalCode,
  GradeLevel
FROM {{ source('StarterPack', 'Schools')}}