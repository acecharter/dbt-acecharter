WITH seis_update_dates AS (
  SELECT
    CASE
      WHEN TableName = 'SeisEmpower' THEN 'ACE Empower Academy'
      WHEN TableName = 'SeisEsperanza' THEN 'ACE Esperanza Middle'
      WHEN TableName = 'SeisInspire' THEN 'ACE Inspire Academy'
      WHEN TableName = 'SeisHighSchool' THEN 'ACE Charter High'
    END AS SchoolName,
    DateTableLastUpdated
  FROM {{ source('GoogleSheetData', 'ManuallyMaintainedFilesTracker')}}
),
  
seis_unioned AS (
  SELECT * FROM {{ source('RawData', 'SeisEmpower')}}

  UNION ALL
  SELECT * FROM {{ source('RawData', 'SeisEsperanza')}}
  
  UNION ALL
  SELECT * FROM {{ source('RawData', 'SeisInspire')}}

  UNION ALL
  SELECT * FROM {{ source('RawData', 'SeisHighSchool')}}
),

seis AS (
  SELECT
    CAST(SEIS_ID AS STRING) AS SeisUniqueId,
    Last_Name AS LastName,
    First_Name AS FirstName,
    CAST(Student_SSID AS STRING) AS StateUniqueId,
    Date_of_Birth AS BirthDate,
    School_of_Attendance AS SchoolName,
    Grade_Code AS GradeLevel,
    Student_Eligibility_Status AS StudentEligibilityStatus,
    Date_of_original_SpEd_Entry AS SpedEntryDate,
    Disability_1_Code AS Disability1Code,
    Disability_1 AS Disability1,
    Disability_2_Code AS Disability2Code,
    Disability_2 AS Disability2
  FROM seis_unioned
)

SELECT
  s.*,
  d.DateTableLastUpdated AS SeisExtractDate
FROM seis AS s
LEFT JOIN seis_update_dates AS d
USING (SchoolName)
