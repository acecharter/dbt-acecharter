WITH schools AS (
  SELECT
    SchoolId,
    NameOfInstitution AS SchoolName,
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
)

SELECT * FROM schools