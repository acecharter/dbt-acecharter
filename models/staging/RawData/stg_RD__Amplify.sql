SELECT
  ELA_District_Name AS DistrictName,
  CASE
    WHEN ELA_School_Name = 'ACE Empower Academy' THEN '116814'
    WHEN ELA_School_Name = 'ACE Esperanza Middle School' THEN '129247'
    WHEN ELA_School_Name = 'ACE Inspire Academy' THEN '131656'
  END AS SchoolId,
  ELA_School_Name AS SchoolName,
  ELA_Class_Name AS ClassName,
  ELA_SIS_ID AS StudentUniqueId,
  ELA_LastName AS LastName,
  ELA_First_Name AS FirstName,
  ELA_Email AS Email,
  ELA_Test_Score_Achieved AS ElaTestScoreAchieved,
  ELA_Test_Score_Potential AS ElaTestScorePotential,
  ELA_Test_Score AS ElaTestScore
FROM {{ source('RawData', 'Amplify2223')}}