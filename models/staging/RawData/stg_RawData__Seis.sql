/*
Note: After extracting and uploading individual school SEIS files to BigQuery, 
these files must be unioned and Seis_Extract_Date column added in BigQuery prior
using BigQuery saved query 'seis_union_and_add_extract_date' prior to running
this model.
*/

  SELECT
    CAST(SEIS_ID AS STRING) AS SeisId,
    Last_Name AS LastName,
    First_Name AS FirstName,
    Date_of_Birth AS BirthDate,
    School_of_Attendance AS SchoolName,
    CAST(Student_SSID AS STRING) AS StateUniqueId,
    Grade_Code AS GradeLevel,
    Student_Eligibility_Status AS StudentEligibilityStatus,
    Seis_Extract_Date AS SeisExtractDate

FROM {{ source('RawData', 'Seis')}}

