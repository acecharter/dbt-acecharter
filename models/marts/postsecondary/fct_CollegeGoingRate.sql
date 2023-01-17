WITH
  cgr AS (
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
      CgrPeriodType,
      HighSchoolCompleters,
      EnrolledInCollegeTotal,
      CollegeGoingRateTotal,
      EnrolledInState,
      EnrolledOutOfState,
      NotEnrolledInCollege,
      EnrolledUc,
      EnrolledCsu,
      EnrolledCcc,
      EnrolledInStatePrivate2And4Year,
      EnrolledOutOfState4YearCollegePublicPrivate,
      EnrolledOutOfState2YearCollegePublicPrivate
    FROM {{ ref('int_CdeCgr__merged')}}
  ),

  final AS (
    SELECT * FROM cgr
    ORDER BY 1, 2, 3, 4, 5, 6, 7
  )

SELECT * FROM final
