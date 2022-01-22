/*
Note: After uploading the individual school SEIS files to BigQuery, the 
Seis_Extract_Date column must be added and populated with a date string
(using 'YYYY-MM-DD' format) in BigQuery prior to running this model, using
the saved project query 'Seis_AddExtractDateToSeisSchoolFiles'.
*/

WITH seis AS (
  SELECT * FROM {{ source('RawData', 'SeisEmpower')}}

  UNION ALL
  SELECT * FROM {{ source('RawData', 'SeisEsperanza')}}
  
  UNION ALL
  SELECT * FROM {{ source('RawData', 'SeisInspire')}}

  UNION ALL
  SELECT * FROM {{ source('RawData', 'SeisHighSchool')}}
)

SELECT
  CAST(SEIS_ID AS STRING) AS SeisId,
  Last_Name AS LastName,
  First_Name AS FirstName,
  Date_of_Birth AS BirthDate,
  School_of_Attendance AS SchoolName,
  CAST(Student_SSID AS STRING) AS SSID,
  Grade_Code AS GradeLevel,
  Student_Eligibility_Status AS StudentEligibilityStatus,
  CAST(Seis_Extract_Date AS DATE) AS SeisExtractDate

FROM seis
