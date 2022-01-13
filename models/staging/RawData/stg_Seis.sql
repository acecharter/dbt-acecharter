--Note: After extracting and uploading files to BigQuery, the Seis_Extract_Date column must be added in BigQuery manually

WITH
seis AS (
  SELECT * FROM {{ source('RawData', 'SeisEmpower')}}
  
  UNION ALL
  SELECT * FROM {{ source('RawData', 'SeisEsperanza')}}
  
  UNION ALL
  SELECT * FROM {{ source('RawData', 'SeisInspire')}}
  
  UNION ALL
  SELECT * FROM {{ source('RawData', 'SeisHS')}}
),

final AS (
  SELECT
    CAST(SEIS_ID AS STRING) AS SeisId,
    Last_Name AS LastName,
    First_Name AS FirstName,
    Date_of_Birth AS BirthDate,
    School_of_Attendance AS SchoolName,
    CAST(Student_SSID AS STRING) AS StateUniqueId,
    Grade_Code AS GradeLevel,
    Seis_Extract_Date

  FROM seis
)

SELECT * FROM final
