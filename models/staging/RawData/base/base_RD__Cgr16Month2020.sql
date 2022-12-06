SELECT
  AcademicYear,
  AggregateLevel,
  FORMAT("%02d", CAST(CountyCode AS INT64)) AS CountyCode,
  FORMAT("%05d", CAST(DistrictCode AS INT64)) AS DistrictCode,
  FORMAT("%07d", CAST(SchoolCode AS INT64)) AS SchoolCode,
  CountyName,
  DistrictName,
  SchoolName,
  TRIM(CharterSchool) AS CharterSchool,
  TRIM(AlternativeSchoolAccountabilityStatus) AS DASS,
  ReportingCategory,
  CompleterType,
  CAST(NULLIF(High_School_Completers, '*') AS INT64) AS HighSchoolCompleters,
  CAST(NULLIF(Enrolled_In_College___Total__16_Months_, '*') AS INT64) AS EnrolledInCollegeTotal16Months,
  ROUND(CAST(NULLIF(College_Going_Rate___Total__16_Months_, '*') AS FLOAT64)/100, 3) AS CollegeGoingRateTotal16Months,
  CAST(NULLIF(Enrolled_In_State__16_Months_, '*') AS INT64) AS EnrolledInState16Months,
  CAST(NULLIF(Enrolled_Out_of_State__16_Months_, '*') AS INT64) AS EnrolledOutOfState16Months,
  CAST(NULLIF(Not_Enrolled_In_College__16_Months_, '*') AS INT64) AS NotEnrolledInCollege16Months,
  CAST(NULLIF(Enrolled_UC__16_Months_, '*') AS INT64) AS EnrolledUc16Months,
  CAST(NULLIF(Enrolled_CSU__16_Months_, '*') AS INT64) AS EnrolledCsu16Months,
  CAST(NULLIF(Enrolled_CCC__16_Months_, '*') AS INT64) AS EnrolledCcc16Months,
  CAST(NULLIF(Enrolled_In_State_Private__2_and_4_Year___16_Months_, '*') AS INT64) AS EnrolledInStatePrivate2And4Year16Months,
  CAST(NULLIF(Enrolled_Out_of_State_4_Year_College__Public_Private___16_Months_, '*') AS INT64) AS EnrolledOutOfState4YearCollegePublicPrivate16Months,
  CAST(NULLIF(Enrolled_Out_of_State_2_Year_College__Public_Private___16_Months_, '*') AS INT64) AS EnrolledOutOfState2YearCollegePublicPrivate16Months
FROM {{ source('RawData', 'CdeCgr16Mo2020')}}