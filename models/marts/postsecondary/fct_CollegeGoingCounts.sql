with 
  unpivoted as (
    {{ dbt_utils.unpivot(
      relation=ref('int_CdeCgr__merged'),
      cast_to='STRING',
      exclude=[
        'AcademicYear',
        'AggregateLevel',
        'EntityType',
        'CountyCode',
        'DistrictCode',
        'SchoolCode',
        'CountyName',
        'DistrictName',
        'SchoolName',
        'CharterSchool',
        'DASS',
        'ReportingCategory',
        'CompleterType',
        'CgrPeriodType',
        'HighSchoolCompleters',
        'EnrolledInCollegeTotal'  
      ],
      remove=[
        'CollegeGoingRateTotal',
        'EnrolledInState',
        'EnrolledOutOfState',
        'NotEnrolledInCollege'
      ],
      field_name='CollegeGoingGroupType',
      value_name='Count'
    ) }}
  )

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
  CollegeGoingGroupType,
  Count,
  CASE WHEN EnrolledInCollegeTotal = 0 THEN NULL ELSE ROUND(CAST(Count AS INT64) / EnrolledInCollegeTotal, 1) END AS PercentOfHsCompleters
FROM unpivoted