select
    AcademicYear,
    case
        when EntityType = 'State' then '00'
        when EntityType = 'County' then CountyCode
        when EntityType = 'District' then DistrictCode
        when EntityType = 'School' then SchoolCode
    end as EntityCode,
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
from {{ ref('int_CdeCgr__unioned') }}
order by 1, 2, 3, 4, 5, 6, 7
