select
    AcademicYear,
    AggregateLevel,
    format('%02d', cast(CountyCode as int64)) as CountyCode,
    format('%05d', cast(DistrictCode as int64)) as DistrictCode,
    format('%07d', cast(SchoolCode as int64)) as SchoolCode,
    CountyName,
    DistrictName,
    SchoolName,
    trim(CharterSchool) as CharterSchool,
    trim(AlternativeSchoolAccountabilityStatus) as DASS,
    ReportingCategory,
    CompleterType,
    cast(nullif(High_School_Completers, '*') as int64) as HighSchoolCompleters,
    cast(nullif(Enrolled_In_College___Total__12_Months_, '*') as int64) as EnrolledInCollegeTotal12Months,
    cast(nullif(College_Going_Rate___Total__12_Months_, '*') as float64) as CollegeGoingRateTotal12Months,
    cast(nullif(Enrolled_In_State__12_Months_, '*') as int64) as EnrolledInState12Months,
    cast(nullif(Enrolled_Out_of_State__12_Months_, '*') as int64) as EnrolledOutOfState12Months,
    cast(nullif(Not_Enrolled_In_College__12_Months_, '*') as int64) as NotEnrolledInCollege12Months,
    cast(nullif(Enrolled_UC__12_Months_, '*') as int64) as EnrolledUc12Months,
    cast(nullif(Enrolled_CSU__12_Months_, '*') as int64) as EnrolledCsu12Months,
    cast(nullif(Enrolled_CCC__12_Months_, '*') as int64) as EnrolledCcc12Months,
    cast(nullif(Enrolled_In_State_Private__2_and_4_Year___12_Months_, '*') as int64) as EnrolledInStatePrivate2And4Year12Months,
    cast(nullif(Enrolled_Out_of_State_4_Year_College__Public_Private___12_Months_, '*') as int64) as EnrolledOutOfState4YearCollegePublicPrivate12Months,
    cast(nullif(Enrolled_Out_of_State_2_Year_College__Public_Private___12_Months_,'*') as int64) as EnrolledOutOfState2YearCollegePublicPrivate12Months
from {{ source('RawData', 'CdeCgr12Mo2018')}}
