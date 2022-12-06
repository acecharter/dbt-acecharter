WITH
  cgr_12 AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityType,
      CountyCode,
      DistrictCode,
      SchoolCode,
      CountyName,
      DistrictName,
      SchoolName,
      CharterSchool,
      DASS,
      ReportingCategory,
      CompleterType,
      '12-month' AS CgrPeriodType,
      HighSchoolCompleters,
      EnrolledInCollegeTotal12Months AS EnrolledInCollegeTotal,
      CollegeGoingRateTotal12Months AS CollegeGoingRateTotal,
      EnrolledInState12Months AS EnrolledInState,
      EnrolledOutOfState12Months AS EnrolledOutOfState,
      NotEnrolledInCollege12Months AS NotEnrolledInCollege,
      EnrolledUc12Months AS EnrolledUc,
      EnrolledCsu12Months AS EnrolledCsu,
      EnrolledCcc12Months AS EnrolledCcc,
      EnrolledInStatePrivate2And4Year12Months AS EnrolledInStatePrivate2And4Year,
      EnrolledOutOfState4YearCollegePublicPrivate12Months AS EnrolledOutOfState4YearCollegePublicPrivate,
      EnrolledOutOfState2YearCollegePublicPrivate12Months AS EnrolledOutOfState2YearCollegePublicPrivate
    FROM {{ ref('stg_RD__CdeCgr12Month')}}
  ),

  cgr_16 AS (
    SELECT
      AcademicYear,
      AggregateLevel,
      EntityType,
      CountyCode,
      DistrictCode,
      SchoolCode,
      CountyName,
      DistrictName,
      SchoolName,
      CharterSchool,
      DASS,
      ReportingCategory,
      CompleterType,
      '16-month' AS CgrPeriodType,
      HighSchoolCompleters,
      EnrolledInCollegeTotal16Months AS EnrolledInCollegeTotal,
      CollegeGoingRateTotal16Months AS CollegeGoingRateTotal,
      EnrolledInState16Months AS EnrolledInState,
      EnrolledOutOfState16Months AS EnrolledOutOfState,
      NotEnrolledInCollege16Months AS NotEnrolledInCollege,
      EnrolledUc16Months AS EnrolledUc,
      EnrolledCsu16Months AS EnrolledCsu,
      EnrolledCcc16Months AS EnrolledCcc,
      EnrolledInStatePrivate2And4Year16Months AS EnrolledInStatePrivate2And4Year,
      EnrolledOutOfState4YearCollegePublicPrivate16Months AS EnrolledOutOfState4YearCollegePublicPrivate,
      EnrolledOutOfState2YearCollegePublicPrivate16Months AS EnrolledOutOfState2YearCollegePublicPrivate
    FROM {{ ref('stg_RD__CdeCgr16Month')}}
  ),

  final AS (
    SELECT * FROM cgr_12
    UNION ALL
    SELECT * FROM cgr_16
  )

SELECT * FROM final
WHERE HighSchoolCompleters IS NOT NULL