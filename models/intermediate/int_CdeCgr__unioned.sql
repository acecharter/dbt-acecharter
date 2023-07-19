with cgr_12 as (
    select
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
        '12-month' as CgrPeriodType,
        HighSchoolCompleters,
        EnrolledInCollegeTotal12Months as EnrolledInCollegeTotal,
        CollegeGoingRateTotal12Months as CollegeGoingRateTotal,
        EnrolledInState12Months as EnrolledInState,
        EnrolledOutOfState12Months as EnrolledOutOfState,
        NotEnrolledInCollege12Months as NotEnrolledInCollege,
        EnrolledUc12Months as EnrolledUc,
        EnrolledCsu12Months as EnrolledCsu,
        EnrolledCcc12Months as EnrolledCcc,
        EnrolledInStatePrivate2And4Year12Months
            as EnrolledInStatePrivate2And4Year,
        EnrolledOutOfState4YearCollegePublicPrivate12Months
            as EnrolledOutOfState4YearCollegePublicPrivate,
        EnrolledOutOfState2YearCollegePublicPrivate12Months
            as EnrolledOutOfState2YearCollegePublicPrivate
    from {{ ref('stg_RD__CdeCgr12Month') }}
),

cgr_16 as (
    select
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
        '16-month' as CgrPeriodType,
        HighSchoolCompleters,
        EnrolledInCollegeTotal16Months as EnrolledInCollegeTotal,
        CollegeGoingRateTotal16Months as CollegeGoingRateTotal,
        EnrolledInState16Months as EnrolledInState,
        EnrolledOutOfState16Months as EnrolledOutOfState,
        NotEnrolledInCollege16Months as NotEnrolledInCollege,
        EnrolledUc16Months as EnrolledUc,
        EnrolledCsu16Months as EnrolledCsu,
        EnrolledCcc16Months as EnrolledCcc,
        EnrolledInStatePrivate2And4Year16Months
            as EnrolledInStatePrivate2And4Year,
        EnrolledOutOfState4YearCollegePublicPrivate16Months
            as EnrolledOutOfState4YearCollegePublicPrivate,
        EnrolledOutOfState2YearCollegePublicPrivate16Months
            as EnrolledOutOfState2YearCollegePublicPrivate
    from {{ ref('stg_RD__CdeCgr16Month') }}
),

final as (
    select * from cgr_12
    union all
    select * from cgr_16
)

select * from final
where HighSchoolCompleters is not null
