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
  SELECT
    * EXCEPT(School_CDS_Code),
    CAST(School_CDS_Code AS STRING) AS School_CDS_Code
  FROM {{ source('RawData', 'SeisEmpower')}}
  UNION ALL
  SELECT
    * EXCEPT(School_CDS_Code),
    CAST(School_CDS_Code AS STRING) AS School_CDS_Code
  FROM {{ source('RawData', 'SeisEsperanza')}}
  UNION ALL
  SELECT
    * EXCEPT(School_CDS_Code),
    CAST(School_CDS_Code AS STRING) AS School_CDS_Code
  FROM {{ source('RawData', 'SeisInspire')}}
  UNION ALL
  SELECT
    * EXCEPT(School_CDS_Code),
    CAST(School_CDS_Code AS STRING) AS School_CDS_Code
  FROM {{ source('RawData', 'SeisHighSchool')}}
),

seis AS (
  SELECT
    CAST(SEIS_ID AS STRING) AS SeisUniqueId,
    CAST(Student_SSID AS STRING) AS StateUniqueId,
    Last_Name AS LastName,
    First_Name AS FirstName,
    Date_of_Birth AS BirthDate,
    CASE
      WHEN School_CDS_Code = '116814' THEN '0116814'
      WHEN School_CDS_Code = '125617' THEN '0125617'
      WHEN School_CDS_Code = '12924a' THEN '0129247'
      WHEN School_CDS_Code = '013165a' THEN '0131656'
    END AS StateSchoolCode,
    School_of_Attendance AS SchoolName,
    Grade_Code AS GradeLevel,
    Student_Eligibility_Status AS StudentEligibilityStatus,
    Date_of_original_SpEd_Entry AS SpedEntryDate,
    Disability_1_Code AS Disability1Code,
    Disability_1 AS Disability1,
    Disability_2_Code AS Disability2Code,
    Disability_2 AS Disability2,
    Date_of_Exit_from_SpEd AS SpedExitDate,
    Student_Exited AS StudentExited
  FROM seis_unioned
)

SELECT
  s.*,
  d.DateTableLastUpdated AS SeisExtractDate
FROM seis AS s
LEFT JOIN seis_update_dates AS d
USING (SchoolName)
