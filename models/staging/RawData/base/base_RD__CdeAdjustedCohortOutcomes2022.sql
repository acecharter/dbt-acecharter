select
  AcademicYear,
  AggregateLevel,
  CountyCode,
  DistrictCode,
  SchoolCode,
  CountyName,
  DistrictName,
  SchoolName,
  CharterSchool,
  DASS,
  ReportingCategory,
  CAST(NULLIF(CohortStudents, '*') AS INT64) AS CohortStudents,
  CAST(NULLIF(Regular_HS_Diploma_Graduates__Count_, '*') AS INT64) AS Regular_HS_Diploma_Graduates__Count_,
  CAST(NULLIF(Regular_HS_Diploma_Graduates__Rate_, '*') AS FLOAT64) AS Regular_HS_Diploma_Graduates__Rate_,
  CAST(NULLIF(Met_UC_CSU_Grad_Req_s__Count_, '*') AS INT64) AS Met_UC_CSU_Grad_Req_s__Count_,
  CAST(NULLIF(Met_UC_CSU_Grad_Req_s__Rate_, '*') AS FLOAT64) AS Met_UC_CSU_Grad_Req_s__Rate_,
  CAST(NULLIF(Seal_of_Biliteracy__Count_, '*') AS INT64) AS Seal_of_Biliteracy__Count_,
  CAST(NULLIF(Seal_of_Biliteracy__Rate_, '*') AS FLOAT64) AS Seal_of_Biliteracy__Rate_,
  CAST(NULLIF(Golden_State_Seal_Merit_Diploma__Count_, '*') AS INT64) AS Golden_State_Seal_Merit_Diploma__Count_,
  CAST(NULLIF(Golden_State_Seal_Merit_Diploma__Rate, '*') AS FLOAT64) AS Golden_State_Seal_Merit_Diploma__Rate,
  CAST(NULLIF(CHSPE_Completer__Count_, '*') AS INT64) AS CHSPE_Completer__Count_,
  CAST(NULLIF(CHSPE_Completer__Rate_, '*') AS FLOAT64) AS CHSPE_Completer__Rate_,
  CAST(NULLIF(Adult_Ed__HS_Diploma__Count_, '*') AS INT64) AS Adult_Ed__HS_Diploma__Count_,
  CAST(NULLIF(Adult_Ed__HS_Diploma__Rate_, '*') AS FLOAT64) AS Adult_Ed__HS_Diploma__Rate_,
  CAST(NULLIF(SPED_Certificate__Count_, '*') AS INT64) AS SPED_Certificate__Count_,
  CAST(NULLIF(SPED_Certificate__Rate_, '*') AS FLOAT64) AS SPED_Certificate__Rate_,
  CAST(NULLIF(GED_Completer__Count_, '*') AS INT64) AS GED_Completer__Count_,
  CAST(NULLIF(GED_Completer__Rate_, '*') AS FLOAT64) AS GED_Completer__Rate_,
  CAST(NULLIF(Other_Transfer__Count_, '*') AS INT64) AS Other_Transfer__Count_,
  CAST(NULLIF(Other_Transfer__Rate_, '*') AS FLOAT64) AS Other_Transfer__Rate_,
  CAST(NULLIF(Dropout__Count_, '*') AS INT64) AS Dropout__Count_,
  CAST(NULLIF(Dropout__Rate_, '*') AS FLOAT64) AS Dropout__Rate_,
  CAST(NULLIF(Still_Enrolled__Count_, '*') AS INT64) AS Still_Enrolled__Count_,
  CAST(NULLIF(Still_Enrolled__Rate_, '*') AS FLOAT64) AS Still_Enrolled__Rate_
from {{ source('RawData', 'CdeAdjustedCohortOutcomes2022')}}
where Met_UC_CSU_Grad_Req_s__Rate_='*'