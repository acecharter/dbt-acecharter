-- Unlike previous years 2021 data did not include records where the CharterSchool and AlternativeSchoolAccountabilityStatus fields were 'All'; so these records are added here to facilitate downstream reporting
with cgr as (
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
    from {{ source('RawData', 'CdeCgr12Mo2021')}}
),

schools as (
    select *
    from cgr
    where AggregateLevel = 'S'
),

charter_all as (
    select
        AcademicYear,
        AggregateLevel,
        CountyCode,
        DistrictCode,
        SchoolCode,
        CountyName,
        DistrictName,
        SchoolName,
        'All' as CharterSchool,
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
    from schools
),

dass_all as (
    select
        AcademicYear,
        AggregateLevel,
        CountyCode,
        DistrictCode,
        SchoolCode,
        CountyName,
        DistrictName,
        SchoolName,
        CharterSchool,
        'All' as DASS,
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
    from schools
),

final as (
    select * from cgr
    union all
    select * from charter_all
    union all
    select * from dass_all
)

select * from final
