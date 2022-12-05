-- Columns dropped: Filler
WITH
  unioned AS (
    SELECT * FROM {{ ref('base_RD__Cgr12Month2017')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Cgr12Month2018')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Cgr12Month2019')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Cgr12Month2020')}}
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
      HighSchoolCompleters,
      EnrolledInCollegeTotal12Months,
      CollegeGoingRateTotal12Months,
      EnrolledInState12Months,
      EnrolledOutOfState12Months,
      NotEnrolledInCollege12Months,
      EnrolledUc12Months,
      EnrolledCsu12Months,
      EnrolledCcc12Months,
      EnrolledInStatePrivate2And4Year12Months,
      EnrolledOutOfState4YearCollegePublicPrivate12Months,
      EnrolledOutOfState2YearCollegePublicPrivate12Months
    FROM unioned
    WHERE
      AggregateLevel = 'T'
      OR (
        AggregateLevel = 'C' 
        AND CountyCode = '43'
      )
      OR DistrictCode = '69427'
  )

SELECT * FROM final