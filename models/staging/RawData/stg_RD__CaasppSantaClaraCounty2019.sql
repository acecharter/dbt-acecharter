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