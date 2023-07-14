with unioned as (
    select * from {{ ref('base_RD__Cgr16Month2017')}}
    union all
    select * from {{ ref('base_RD__Cgr16Month2018')}}
    union all
    select * from {{ ref('base_RD__Cgr16Month2019')}}
    union all
    select * from {{ ref('base_RD__Cgr16Month2020')}}
    union all
    select * from {{ ref('base_RD__Cgr16Month2021')}}
),

final as (
    select
        AcademicYear,
        AggregateLevel,
        case AggregateLevel
            when 'T' then 'State'
            when 'C' then 'County'
            when 'D' then 'District'
            when 'S' then 'School'
        end as EntityType,
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
        round(CollegeGoingRateTotal16Months / 100, 3) as CollegeGoingRateTotal16Months,
        EnrolledInState16Months,
        EnrolledOutOfState16Months,
        NotEnrolledInCollege16Months,
        EnrolledUc16Months,
        EnrolledCsu16Months,
        EnrolledCcc16Months,
        EnrolledInStatePrivate2And4Year16Months,
        EnrolledOutOfState4YearCollegePublicPrivate16Months,
        EnrolledOutOfState2YearCollegePublicPrivate16Months
    from unioned
    where
        AggregateLevel = 'T'
        or (
            AggregateLevel = 'C' 
            and CountyCode = '43'
        )
        or DistrictCode = '69427'
)

select * from final