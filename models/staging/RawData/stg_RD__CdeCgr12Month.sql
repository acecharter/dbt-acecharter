with unioned as (
        select * from {{ ref('base_RD__Cgr12Month2017')}}
        union all
        select * from {{ ref('base_RD__Cgr12Month2018')}}
        union all
        select * from {{ ref('base_RD__Cgr12Month2019')}}
        union all
        select * from {{ ref('base_RD__Cgr12Month2020')}}
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
            EnrolledInCollegeTotal12Months,
            round(CollegeGoingRateTotal12Months / 100, 3) as CollegeGoingRateTotal12Months,
            EnrolledInState12Months,
            EnrolledOutOfState12Months,
            NotEnrolledInCollege12Months,
            EnrolledUc12Months,
            EnrolledCsu12Months,
            EnrolledCcc12Months,
            EnrolledInStatePrivate2And4Year12Months,
            EnrolledOutOfState4YearCollegePublicPrivate12Months,
            EnrolledOutOfState2YearCollegePublicPrivate12Months
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