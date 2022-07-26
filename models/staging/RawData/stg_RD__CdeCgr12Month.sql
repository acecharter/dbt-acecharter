-- Columns dropped: Filler
WITH
  cgr_2017 AS (
    SELECT *
    FROM {{ source('RawData', 'CdeCgr12Mo2017')}}
  ),
  
  cgr_2018 AS (
    SELECT *
    FROM {{ source('RawData', 'CdeCgr12Mo2018')}}
  ),

  unioned AS (
    SELECT * FROM cgr_2017
    UNION ALL
    SELECT * FROM cgr_2018
  ),

  final AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      CASE
        WHEN AggregateLevel = 'T' THEN 'State'
        WHEN AggregateLevel = 'C' THEN 'County'
        WHEN AggregateLevel = 'D' THEN 'District'
        WHEN AggregateLevel = 'S' THEN 'School'
      END AS EntityType,
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
      CAST(NULLIF(Enrolled_In_College___Total__12_Months_, '*') AS INT64) AS EnrolledInCollegeTotal12Months,
      ROUND(CAST(NULLIF(College_Going_Rate___Total__12_Months_, '*') AS FLOAT64)/100, 3) AS CollegeGoingRateTotal12Months,
      CAST(NULLIF(Enrolled_In_State__12_Months_, '*') AS INT64) AS EnrolledInState12Months,
      CAST(NULLIF(Enrolled_Out_of_State__12_Months_, '*') AS INT64) AS EnrolledOutOfState12Months,
      CAST(NULLIF(Not_Enrolled_In_College__12_Months_, '*') AS INT64) AS NotEnrolledInCollege12Months,
      CAST(NULLIF(Enrolled_UC__12_Months_, '*') AS INT64) AS EnrolledUc12Months,
      CAST(NULLIF(Enrolled_CSU__12_Months_, '*') AS INT64) AS EnrolledCsu12Months,
      CAST(NULLIF(Enrolled_CCC__12_Months_, '*') AS INT64) AS EnrolledCcc12Months,
      CAST(NULLIF(Enrolled_In_State_Private__2_and_4_Year___12_Months_, '*') AS INT64) AS EnrolledInStatePrivate2And4Year12Months,
      CAST(NULLIF(Enrolled_Out_of_State_4_Year_College__Public_Private___12_Months_, '*') AS INT64) AS EnrolledOutOfState4YearCollegePublicPrivate12Months,
      CAST(NULLIF(Enrolled_Out_of_State_2_Year_College__Public_Private___12_Months_, '*') AS INT64) AS EnrolledOutOfState2YearCollegePublicPrivate12Months
    FROM unioned
    WHERE
      AggregateLevel = 'T'
      OR (
        AggregateLevel = 'C' 
        AND CountyCode = 43
      )
      OR DistrictCode = '69427'
  )

SELECT * FROM final