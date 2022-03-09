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