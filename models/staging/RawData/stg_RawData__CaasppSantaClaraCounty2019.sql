-- Columns dropped: Filler

SELECT
  FORMAT("%02d", County_Code) AS CountyCode,
  FORMAT("%05d", District_Code) AS DistrictCode,
  FORMAT("%07d", School_Code) AS SchoolCode,
  Test_Year AS TestYear,
  CONCAT(
    CAST(Test_Year - 1 AS STRING),
    '-',
    CAST(Test_Year - 2000 AS STRING)
  ) AS SchoolYear,
  CAST(NULL AS STRING) AS TypeId,
  CAST(Subgroup_ID AS STRING) AS DemographicId,
  Test_Type AS TestType,
  Total_Tested_at_Entity_Level AS TotalTestedAtReportingLevel,
  Total_Tested_with_Scores AS TotalTestedWithScoresAtReportingLevel,
  Grade AS GradeLevel,
  CAST(Test_Id AS STRING) AS TestId,
  CAASPP_Reported_Enrollment AS StudentsEnrolled,
  Students_Tested AS StudentsTested,
  Mean_Scale_Score AS MeanScaleScore,
  ROUND(Percentage_Standard_Exceeded/100, 4) AS PctStandardExceeded,
  ROUND(Percentage_Standard_Met/100, 4) AS PctStandardMet,
  ROUND(Percentage_Standard_Met_and_Above/100, 4) AS PctStandardMetAndAbove,
  ROUND(Percentage_Standard_Nearly_Met/100, 4) AS PctStandardNearlyMet,
  ROUND(Percentage_Standard_Not_Met/100, 4) AS PctStandardNotMet,
  Students_with_Scores AS StudentsWithScores,
  ROUND(Area_1_Percentage_Above_Standard/100, 4) AS Area1PctAboveStandard,
  ROUND(Area_1_Percentage_Near_Standard/100, 4) AS Area1PctNearStandard,
  ROUND(Area_1_Percentage_Below_Standard/100, 4) AS Area1PctBelowStandard,
  ROUND(Area_2_Percentage_Above_Standard/100, 4) AS Area2PctAboveStandard,
  ROUND(Area_2_Percentage_Near_Standard/100, 4) AS Area2PctNearStandard,
  ROUND(Area_2_Percentage_Below_Standard/100, 4) AS Area2PctBelowStandard,
  ROUND(Area_3_Percentage_Above_Standard/100, 4) AS Area3PctAboveStandard,
  ROUND(Area_3_Percentage_Near_Standard/100, 4) AS Area3PctNearStandard,
  ROUND(Area_3_Percentage_Below_Standard/100, 4) AS Area3PctBelowStandard,
  ROUND(Area_4_Percentage_Above_Standard/100, 4) AS Area4PctAboveStandard,
  ROUND(Area_4_Percentage_Near_Standard/100, 4) AS Area4PctNearStandard,
  ROUND(Area_4_Percentage_Below_Standard/100, 4) AS Area4PctBelowStandard

FROM {{ source('RawData', 'CaasppSantaClaraCounty2019')}}