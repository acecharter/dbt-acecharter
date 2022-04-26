SELECT
  County_Code,
  District_Code,
  School_Code,
  Filler,
  Test_Year,
  Subgroup_ID AS Student_Group_ID,
  Test_Type,
  CAST(NULLIF(Total_Tested_At_Entity_Level, '*') AS FLOAT64) AS Total_Tested_At_Reporting_Level,
  CAST(NULLIF(Total_Tested_with_Scores, '*') AS FLOAT64) AS Total_Tested_with_Scores_at_Reporting_Level,
  Grade,
  Test_Id,
  CAST(NULLIF(CAASPP_Reported_Enrollment, '*') AS FLOAT64) AS Students_Enrolled,
  CAST(NULLIF(Students_Tested, '*') AS FLOAT64) AS Students_Tested,
  CAST(CASE WHEN Mean_Scale_Score NOT IN ('*','','0.0') THEN Mean_Scale_Score END AS FLOAT64) AS Mean_Scale_Score,
  CAST(NULLIF(Percentage_Standard_Exceeded, '*') AS FLOAT64) AS Percentage_Standard_Exceeded,
  CAST(NULLIF(Percentage_Standard_Met, '*') AS FLOAT64) AS Percentage_Standard_Met,
  CAST(NULLIF(Percentage_Standard_Met_and_Above, '*') AS FLOAT64) AS Percentage_Standard_Met_and_Above,
  CAST(NULLIF(Percentage_Standard_Nearly_Met, '*') AS FLOAT64) AS Percentage_Standard_Nearly_Met,
  CAST(NULLIF(Percentage_Standard_Not_Met, '*') AS FLOAT64) AS Percentage_Standard_Not_Met,
  CAST(NULLIF(Students_with_Scores, '*') AS INT64) AS Students_with_Scores,
  CAST(NULLIF(Area_1_Percentage_Above_Standard, '*') AS FLOAT64) AS Area_1_Percentage_Above_Standard,
  CAST(NULLIF(Area_1_Percentage_Near_Standard, '*') AS FLOAT64) AS Area_1_Percentage_Near_Standard,
  CAST(NULLIF(Area_1_Percentage_Below_Standard, '*') AS FLOAT64) AS Area_1_Percentage_Below_Standard,
  CAST(NULLIF(Area_2_Percentage_Above_Standard, '*') AS FLOAT64) AS Area_2_Percentage_Above_Standard,
  CAST(NULLIF(Area_2_Percentage_Near_Standard, '*') AS FLOAT64) AS Area_2_Percentage_Near_Standard,
  CAST(NULLIF(Area_2_Percentage_Below_Standard, '*') AS FLOAT64) AS Area_2_Percentage_Below_Standard,
  CAST(NULLIF(Area_3_Percentage_Above_Standard, '*') AS FLOAT64) AS Area_3_Percentage_Above_Standard,
  CAST(NULLIF(Area_3_Percentage_Near_Standard, '*') AS FLOAT64) AS Area_3_Percentage_Near_Standard,
  CAST(NULLIF(Area_3_Percentage_Below_Standard, '*') AS FLOAT64) AS Area_3_Percentage_Below_Standard,
  CAST(NULLIF(Area_4_Percentage_Above_Standard, '*') AS FLOAT64) AS Area_4_Percentage_Above_Standard,
  CAST(NULLIF(Area_4_Percentage_Near_Standard, '*') AS FLOAT64) AS Area_4_Percentage_Near_Standard,
  CAST(NULLIF(Area_4_Percentage_Below_Standard, '*') AS FLOAT64) AS Area_4_Percentage_Below_Standard,
  CAST(NULL AS STRING) AS TypeId
FROM {{ source('RawData', 'Caaspp2017')}}
