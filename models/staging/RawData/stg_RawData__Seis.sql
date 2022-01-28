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
    Date_of_Birth AS BirthDate,
    School_of_Attendance AS SchoolName,
    CAST(Student_SSID AS STRING) AS SSID,
    Grade_Code AS GradeLevel,
    Student_Eligibility_Status AS StudentEligibilityStatus
  FROM seis_unioned
)

SELECT
  s.*,
  d.DateTableLastUpdated AS SeisExtractDate
FROM seis AS s
LEFT JOIN seis_update_dates AS d
USING (SchoolName)
