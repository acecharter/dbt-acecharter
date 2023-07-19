-- Unlike previous years 2021 data did not include records  where the CharterSchool and AlternativeSchoolAccountabilityStatus fields were 'All'; so these records are added here to facilitate downstream reporting
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
        cast(nullif(Enrolled_In_College___Total__16_Months_, '*') as int64) as EnrolledInCollegeTotal16Months,
        cast(nullif(College_Going_Rate___Total__16_Months_, '*') as float64) as CollegeGoingRateTotal16Months,
        cast(nullif(Enrolled_In_State__16_Months_, '*') as int64) as EnrolledInState16Months,
        cast(nullif(Enrolled_Out_of_State__16_Months_, '*') as int64) as EnrolledOutOfState16Months,
        cast(nullif(Not_Enrolled_In_College__16_Months_, '*') as int64) as NotEnrolledInCollege16Months,
        cast(nullif(Enrolled_UC__16_Months_, '*') as int64) as EnrolledUc16Months,
        cast(nullif(Enrolled_CSU__16_Months_, '*') as int64) as EnrolledCsu16Months,
        cast(nullif(Enrolled_CCC__16_Months_, '*') as int64) as EnrolledCcc16Months,
        cast(nullif(Enrolled_In_State_Private__2_and_4_Year___16_Months_, '*') as int64) as EnrolledInStatePrivate2And4Year16Months,
        cast(nullif(Enrolled_Out_of_State_4_Year_College__Public_Private___16_Months_, '*') as int64) as EnrolledOutOfState4YearCollegePublicPrivate16Months,
        cast(nullif(Enrolled_Out_of_State_2_Year_College__Public_Private___16_Months_,'*') as int64) as EnrolledOutOfState2YearCollegePublicPrivate16Months
    from {{ source('RawData', 'CdeCgr16Mo2021')}}
),

schools as (
    select *
    from cgr
    where AggregateLevel = 'S'
),

charter_dass_all as (
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
        'All' as DASS,
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
    from schools
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
    from schools
),

final as (
    select * from cgr
    union all
    select * from charter_dass_all
    union all
    select * from charter_all
    union all
    select * from dass_all
)

select * from final
