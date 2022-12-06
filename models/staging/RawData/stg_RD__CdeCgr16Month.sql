-- Columns dropped: Filler
WITH
  unioned AS (
    SELECT * FROM {{ ref('base_RD__Cgr16Month2017')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Cgr16Month2018')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Cgr16Month2019')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Cgr16Month2020')}}
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
      EnrolledInCollegeTotal16Months,
      CollegeGoingRateTotal16Months,
      EnrolledInState16Months,
      EnrolledOutOfState16Months,
      NotEnrolledInCollege16Months,
      EnrolledUc16Months,
      EnrolledCsu16Months,
      EnrolledCcc16Months,
      EnrolledInStatePrivate2And4Year16Months,
      EnrolledOutOfState4YearCollegePublicPrivate16Months,
      EnrolledOutOfState2YearCollegePublicPrivate16Months
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