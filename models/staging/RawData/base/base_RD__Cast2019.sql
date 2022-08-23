SELECT 
  FORMAT("%02d", County_Code) AS CountyCode,
  FORMAT("%05d", District_Code) AS DistrictCode,
  FORMAT("%07d", School_Code) AS SchoolCode,
  Filler,
  CAST(Test_Year AS INT64) AS TestYear,
  CAST(Demographic_ID AS STRING) AS DemographicId,
  Test_Type AS TestType,
  CAST(NULLIF(Total_Number_Tested_At_Entity_Level_and_Demographic, '*') AS INT64) AS TotalTestedAtReportingLevel,
  CAST(NULLIF(Total_Number_Tested_at_this_Demographic_with_Valid_Scores, '*') AS INT64) AS TotalTestedWithScoresAtReportingLevel,
  CAST(Grade AS INT64) GradeLevel,
  CAST(Test_ID AS STRING) AS TestId,
  CAST(NULLIF(CAST_Reported_Enrollment, '*') AS INT64) AS StudentsEnrolled,
  CAST(NULLIF(Total_Number_of_Students_Tested, '*') AS INT64) AS StudentsTested,
  CAST(CASE WHEN Mean_Scale_Score NOT IN ('*','','0.0') THEN Mean_Scale_Score END AS FLOAT64) AS MeanScaleScore,
  ROUND(CAST(NULLIF(Percentage_Standard_Exceeded, '*') AS FLOAT64)/100, 4) AS PctStandardExceeded,
  ROUND(CAST(NULLIF(Percentage_Standard_Met, '*') AS FLOAT64)/100, 4) AS PctStandardMet,
  ROUND(CAST(NULLIF(Percentage_Standard_Met_and_Above, '*') AS FLOAT64)/100, 4) AS PctStandardMetandAbove,
  ROUND(CAST(NULLIF(Percentage_Standard_Nearly_Met, '*') AS FLOAT64)/100, 4) AS PctStandardNearlyMet,
  ROUND(CAST(NULLIF(Percentage_Standard_Not_Met, '*') AS FLOAT64)/100, 4) AS PctStandardNotMet,
  CAST(NULLIF(Total_Number_of_Students_with_Valid_Scores, '*') AS INT64) AS StudentsWithScores,
  ROUND(CAST(NULLIF(Life_Sciences_Domain_Percent_Below_Standard, '*') AS FLOAT64)/100, 4) AS LifeSciencesDomainPercentBelowStandard,
  ROUND(CAST(NULLIF(Life_Sciences_Domain_Percent_Near_Standard, '*') AS FLOAT64)/100, 4) AS LifeSciencesDomainPercentNearStandard,
  ROUND(CAST(NULLIF(Life_Sciences_Domain_Percent_Above_Standard, '*') AS FLOAT64)/100, 4) AS LifeSciencesDomainPercentAboveStandard,
  ROUND(CAST(NULLIF(Physical_Sciences_Domain_Percent_Below_Standard, '*') AS FLOAT64)/100, 4) AS PhysicalSciencesDomainPercentBelowStandard,
  ROUND(CAST(NULLIF(Physical_Sciences_Domain_Percent_Near_Standard, '*') AS FLOAT64)/100, 4) AS PhysicalSciencesDomainPercentNearStandard,
  ROUND(CAST(NULLIF(Physical_Sciences_Domain_Percent_Above_Standard, '*') AS FLOAT64)/100, 4) AS PhysicalSciencesDomainPercentAboveStandard,
  ROUND(CAST(NULLIF(Earth_and_Space_Sciences_Domain_Percent_Below_Standard, '*') AS FLOAT64)/100, 4) AS EarthAndSpaceSciencesDomainPercentBelowStandard,
  ROUND(CAST(NULLIF(Earth_and_Space_Sciences_Domain_Percent_Near_Standard, '*') AS FLOAT64)/100, 4) AS EarthAndSpaceSciencesDomainPercentNearStandard,
  ROUND(CAST(NULLIF(Earth_and_Space_Sciences_Domain_Percent_Above_Standard, '*') AS FLOAT64)/100, 4) AS EarthAndSpaceSciencesDomainPercentAboveStandard,
  CAST(NULL AS STRING) AS TypeId
FROM {{ source('RawData', 'Cast2019')}}