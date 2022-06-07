WITH
  twelve AS (
    SELECT
      AcademicYear,
      CASE
        WHEN EntityType = 'State' THEN '00'
        WHEN EntityType = 'County' THEN CountyCode
        WHEN EntityType = 'District' THEN DistrictCode
        WHEN EntityType = 'School' THEN SchoolCode
      END AS EntityCode,     
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

  unioned AS (
    SELECT * FROM twelve
  ),

  final AS (
    SELECT *
    FROM unioned
    WHERE HighSchoolCompleters IS NOT NULL
    ORDER BY 1, 2, 3, 4, 5, 6, 7
  )


SELECT * FROM final
