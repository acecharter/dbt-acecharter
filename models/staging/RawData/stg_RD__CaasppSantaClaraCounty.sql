-- Columns dropped: Filler
WITH 
  caaspp_2015 AS (
    SELECT
      County_Code,
      District_Code,
      School_Code,
      Filler,
      Test_Year,
      Subgroup_ID,
      Test_Type,
      Grade,
      Test_Id,
      Students_Tested,
      CAST(REPLACE(Mean_Scale_Score, '*', NULL) AS FLOAT64) AS Mean_Scale_Score,
      CAST(REPLACE(Percentage_Standard_Exceeded, '*', NULL) AS FLOAT64) AS Percentage_Standard_Exceeded,
      CAST(REPLACE(Percentage_Standard_Met, '*', NULL) AS FLOAT64) AS Percentage_Standard_Met,
      CAST(REPLACE(Percentage_Standard_Met_and_Above, '*', NULL) AS FLOAT64) AS Percentage_Standard_Met_and_Above,
      CAST(REPLACE(Percentage_Standard_Nearly_Met, '*', NULL) AS FLOAT64) AS Percentage_Standard_Nearly_Met,
      CAST(REPLACE(Percentage_Standard_Not_Met, '*', NULL) AS FLOAT64) AS Percentage_Standard_Not_Met,
      Students_with_Scores,
      CAST(REPLACE(Area_1_Percentage_Above_Standard, '*', NULL) AS FLOAT64) AS Area_1_Percentage_Above_Standard,
      CAST(REPLACE(Area_1_Percentage_At_Or_Near_Standard, '*', NULL) AS FLOAT64) AS Area_1_Percentage_Near_Standard,
      CAST(REPLACE(Area_1_Percentage_Below_Standard, '*', NULL) AS FLOAT64) AS Area_1_Percentage_Below_Standard,
      CAST(REPLACE(Area_2_Percentage_Above_Standard, '*', NULL) AS FLOAT64) AS Area_2_Percentage_Above_Standard,
      CAST(REPLACE(Area_2_Percentage_At_Or_Near_Standard, '*', NULL) AS FLOAT64) AS Area_2_Percentage_Near_Standard,
      CAST(REPLACE(Area_2_Percentage_Below_Standard, '*', NULL) AS FLOAT64) AS Area_2_Percentage_Below_Standard,
      CAST(REPLACE(Area_3_Percentage_Above_Standard, '*', NULL) AS FLOAT64) AS Area_3_Percentage_Above_Standard,
      CAST(REPLACE(Area_3_Percentage_At_Or_Near_Standard, '*', NULL) AS FLOAT64) AS Area_3_Percentage_Near_Standard,
      CAST(REPLACE(Area_3_Percentage_Below_Standard, '*', NULL) AS FLOAT64) AS Area_3_Percentage_Below_Standard,
      CAST(REPLACE(Area_4_Percentage_Above_Standard, '*', NULL) AS FLOAT64) AS Area_4_Percentage_Above_Standard,
      CAST(REPLACE(Area_4_Percentage_At_Or_Near_Standard, '*', NULL) AS FLOAT64) AS Area_4_Percentage_Near_Standard,
      CAST(REPLACE(Area_4_Percentage_Below_Standard, '*', NULL) AS FLOAT64) AS Area_4_Percentage_Below_Standard,
      CAST(NULL AS STRING) AS TypeId,
      Total_Tested_At_Entity_Level AS Total_Tested_At_Reporting_Level,
      CAST(NULL AS INT64) AS Total_Tested_with_Scores_at_Reporting_Level,
      CAASPP_Reported_Enrollment AS Students_Enrolled
    FROM {{ source('RawData', 'CaasppSantaClaraCounty2015')}}
  ),

  caaspp_2016 AS (
    SELECT
      County_Code,
      District_Code,
      School_Code,
      Filler,
      Test_Year,
      Subgroup_ID,
      Test_Type,
      Grade,
      Test_Id,
      Students_Tested,
      CAST(REPLACE(Mean_Scale_Score, '*', NULL) AS FLOAT64) AS Mean_Scale_Score,
      CAST(REPLACE(Percentage_Standard_Exceeded, '*', NULL) AS FLOAT64) AS Percentage_Standard_Exceeded,
      CAST(REPLACE(Percentage_Standard_Met, '*', NULL) AS FLOAT64) AS Percentage_Standard_Met,
      CAST(REPLACE(Percentage_Standard_Met_and_Above, '*', NULL) AS FLOAT64) AS Percentage_Standard_Met_and_Above,
      CAST(REPLACE(Percentage_Standard_Nearly_Met, '*', NULL) AS FLOAT64) AS Percentage_Standard_Nearly_Met,
      CAST(REPLACE(Percentage_Standard_Not_Met, '*', NULL) AS FLOAT64) AS Percentage_Standard_Not_Met,
      Students_with_Scores,
      CAST(REPLACE(Area_1_Percentage_Above_Standard, '*', NULL) AS FLOAT64) AS Area_1_Percentage_Above_Standard,
      CAST(REPLACE(Area_1_Percentage_Near_Standard, '*', NULL) AS FLOAT64) AS Area_1_Percentage_Near_Standard,
      CAST(REPLACE(Area_1_Percentage_Below_Standard, '*', NULL) AS FLOAT64) AS Area_1_Percentage_Below_Standard,
      CAST(REPLACE(Area_2_Percentage_Above_Standard, '*', NULL) AS FLOAT64) AS Area_2_Percentage_Above_Standard,
      CAST(REPLACE(Area_2_Percentage_Near_Standard, '*', NULL) AS FLOAT64) AS Area_2_Percentage_Near_Standard,
      CAST(REPLACE(Area_2_Percentage_Below_Standard, '*', NULL) AS FLOAT64) AS Area_2_Percentage_Below_Standard,
      CAST(REPLACE(Area_3_Percentage_Above_Standard, '*', NULL) AS FLOAT64) AS Area_3_Percentage_Above_Standard,
      CAST(REPLACE(Area_3_Percentage_Near_Standard, '*', NULL) AS FLOAT64) AS Area_3_Percentage_Near_Standard,
      CAST(REPLACE(Area_3_Percentage_Below_Standard, '*', NULL) AS FLOAT64) AS Area_3_Percentage_Below_Standard,
      CAST(REPLACE(Area_4_Percentage_Above_Standard, '*', NULL) AS FLOAT64) AS Area_4_Percentage_Above_Standard,
      CAST(REPLACE(Area_4_Percentage_Near_Standard, '*', NULL) AS FLOAT64) AS Area_4_Percentage_Near_Standard,
      CAST(REPLACE(Area_4_Percentage_Below_Standard, '*', NULL) AS FLOAT64) AS Area_4_Percentage_Below_Standard,
      CAST(NULL AS STRING) AS TypeId,
      Total_Tested_At_Entity_Level AS Total_Tested_At_Reporting_Level,
      Total_Tested_with_Scores AS Total_Tested_with_Scores_at_Reporting_Level,
      CAASPP_Reported_Enrollment AS Students_Enrolled
    FROM {{ source('RawData', 'CaasppSantaClaraCounty2016')}}
  ),

  caaspp_2017 AS (
    SELECT
      County_Code,
      District_Code,
      School_Code,
      Filler,
      Test_Year,
      Subgroup_ID,
      Test_Type,
      Grade,
      Test_Id,
      CAST(REPLACE(Students_Tested, '*', NULL) AS INT64) AS Students_Tested,
      CAST(REPLACE(Mean_Scale_Score, '*', NULL) AS FLOAT64) AS Mean_Scale_Score,
      CAST(REPLACE(Percentage_Standard_Exceeded, '*', NULL) AS FLOAT64) AS Percentage_Standard_Exceeded,
      CAST(REPLACE(Percentage_Standard_Met, '*', NULL) AS FLOAT64) AS Percentage_Standard_Met,
      CAST(REPLACE(Percentage_Standard_Met_and_Above, '*', NULL) AS FLOAT64) AS Percentage_Standard_Met_and_Above,
      CAST(REPLACE(Percentage_Standard_Nearly_Met, '*', NULL) AS FLOAT64) AS Percentage_Standard_Nearly_Met,
      CAST(REPLACE(Percentage_Standard_Not_Met, '*', NULL) AS FLOAT64) AS Percentage_Standard_Not_Met,
      CAST(REPLACE(Students_with_Scores, '*', NULL) AS INT64) AS Students_with_Scores,
      CAST(REPLACE(Area_1_Percentage_Above_Standard, '*', NULL) AS FLOAT64) AS Area_1_Percentage_Above_Standard,
      CAST(REPLACE(Area_1_Percentage_Near_Standard, '*', NULL) AS FLOAT64) AS Area_1_Percentage_Near_Standard,
      CAST(REPLACE(Area_1_Percentage_Below_Standard, '*', NULL) AS FLOAT64) AS Area_1_Percentage_Below_Standard,
      CAST(REPLACE(Area_2_Percentage_Above_Standard, '*', NULL) AS FLOAT64) AS Area_2_Percentage_Above_Standard,
      CAST(REPLACE(Area_2_Percentage_Near_Standard, '*', NULL) AS FLOAT64) AS Area_2_Percentage_Near_Standard,
      CAST(REPLACE(Area_2_Percentage_Below_Standard, '*', NULL) AS FLOAT64) AS Area_2_Percentage_Below_Standard,
      CAST(REPLACE(Area_3_Percentage_Above_Standard, '*', NULL) AS FLOAT64) AS Area_3_Percentage_Above_Standard,
      CAST(REPLACE(Area_3_Percentage_Near_Standard, '*', NULL) AS FLOAT64) AS Area_3_Percentage_Near_Standard,
      CAST(REPLACE(Area_3_Percentage_Below_Standard, '*', NULL) AS FLOAT64) AS Area_3_Percentage_Below_Standard,
      CAST(REPLACE(Area_4_Percentage_Above_Standard, '*', NULL) AS FLOAT64) AS Area_4_Percentage_Above_Standard,
      CAST(REPLACE(Area_4_Percentage_Near_Standard, '*', NULL) AS FLOAT64) AS Area_4_Percentage_Near_Standard,
      CAST(REPLACE(Area_4_Percentage_Below_Standard, '*', NULL) AS FLOAT64) AS Area_4_Percentage_Below_Standard,
      CAST(NULL AS STRING) AS TypeId,
      CAST(REPLACE(Total_Tested_At_Entity_Level, '*', NULL) AS INT64) AS Total_Tested_At_Reporting_Level,
      CAST(REPLACE(Total_Tested_with_Scores, '*', NULL) AS INT64) AS Total_Tested_with_Scores_at_Reporting_Level,
      CAST(REPLACE(CAASPP_Reported_Enrollment, '*', NULL) AS INT64) AS Students_Enrolled
    FROM {{ source('RawData', 'CaasppSantaClaraCounty2017')}}
  ),

  caaspp_2018 AS (
    SELECT
      * EXCEPT(
        Total_Tested_with_Scores,
        Total_Tested_At_Entity_Level,
        CAASPP_Reported_Enrollment   
      ),
      CAST(NULL AS STRING) AS TypeId,
      Total_Tested_At_Entity_Level AS Total_Tested_At_Reporting_Level,
      Total_Tested_with_Scores AS Total_Tested_with_Scores_at_Reporting_Level,
      CAASPP_Reported_Enrollment AS Students_Enrolled
    FROM {{ source('RawData', 'CaasppSantaClaraCounty2018')}}
  ),

  caaspp_2019 AS (
    SELECT 
      * EXCEPT(
        Total_Tested_with_Scores,
        Total_Tested_At_Entity_Level,
        CAASPP_Reported_Enrollment 
      ),
      CAST(NULL AS STRING) AS TypeId,
      Total_Tested_At_Entity_Level AS Total_Tested_At_Reporting_Level,
      Total_Tested_with_Scores AS Total_Tested_with_Scores_at_Reporting_Level,
      CAASPP_Reported_Enrollment AS Students_Enrolled
    FROM {{ source('RawData', 'CaasppSantaClaraCounty2019')}}
  ),

  caaspp_2021 AS (
    SELECT
      * EXCEPT(
        Type_ID,
        Total_Tested_At_Reporting_Level,
        Total_Tested_with_Scores_at_Reporting_Level,
        Students_Enrolled
      ),
      FORMAT("%02d", Type_ID) AS TypeId,
      Total_Tested_At_Reporting_Level,
      Total_Tested_with_Scores_at_Reporting_Level,
      Students_Enrolled
    FROM {{ source('RawData', 'CaasppSantaClaraCounty2021')}}
  ),

  caaspp_unioned AS (
    SELECT * FROM caaspp_2015
    UNION ALL
    SELECT * FROM caaspp_2016
    UNION ALL
    SELECT * FROM caaspp_2017
    UNION ALL
    SELECT * FROM caaspp_2018
    UNION ALL
    SELECT * FROM caaspp_2019
    UNION ALL
    SELECT * FROM caaspp_2021
  ),

  final AS (
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
      TypeId,
      CAST(Subgroup_ID AS STRING) AS DemographicId,
      Test_Type AS TestType,
      Total_Tested_At_Reporting_Level AS TotalTestedAtReportingLevel,
      Total_Tested_with_Scores_at_Reporting_Level AS TotalTestedWithScoresAtReportingLevel,
      Grade AS GradeLevel,
      CAST(Test_Id AS STRING) AS TestId,
      Students_Enrolled AS StudentsEnrolled,
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

    FROM caaspp_unioned
  )

SELECT * FROM final