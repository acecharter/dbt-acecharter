with susp_1718 as (
    select * from {{ source('RawData', 'CdeSusp1718') }}
),

susp_1819 as (
    select * from {{ source('RawData', 'CdeSusp1819') }}
),

susp_1920 as (
    select * from {{ source('RawData', 'CdeSusp1920') }}
),

susp_2021 as (
    select * from {{ source('RawData', 'CdeSusp2021') }}
),

susp_2122 as (
    select * from {{ source('RawData', 'CdeSusp2122') }}
),

unioned as (
    select * from susp_1718
    union all
    select * from susp_1819
    union all
    select * from susp_1920
    union all
    select * from susp_2021
    union all
    select * from susp_2122
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
        format('%02d', cast(CountyCode as int64)) as CountyCode,
        format('%05d', cast(DistrictCode as int64)) as DistrictCode,
        format('%07d', cast(SchoolCode as int64)) as SchoolCode,
        CountyName,
        DistrictName,
        SchoolName,
        trim(CharterYN) as CharterSchool,
        ReportingCategory,
        cast(nullif(Cumulative_Enrollment, '*') as int64)
            as CumulativeEnrollment,
        cast(nullif(Total_Suspensions, '*') as int64) as TotalSuspensions,
        cast(
            nullif(
                Unduplicated_Count_of_Students_Suspended__Total_, '*'
            ) as int64
        ) as UnduplicatedCountOfStudentsSuspendedTotal,
        cast(
            nullif(
                Unduplicated_Count_of_Students_Suspended__Defiance_Only_, '*'
            ) as int64
        ) as UnduplicatedCountOfStudentsSuspendedDefianceOnly,
        round(cast(nullif(Suspension_Rate__Total_, '*') as float64) / 100, 3)
            as SuspensionRateTotal,
        cast(nullif(Suspension_Count_Violent_Incident__Injury_, '*') as int64)
            as SuspensionCountViolentIncidentInjury,
        cast(
            nullif(Suspension_Count_Violent_Incident__No_Injury_, '*') as int64
        ) as SuspensionCountViolentIncidentNoInjury,
        cast(nullif(Suspension_Count_Weapons_Possession, '*') as int64)
            as SuspensionCountWeaponsPossession,
        cast(nullif(Suspension_Count_Illicit_Drug_Related, '*') as int64)
            as SuspensionCountIllicitDrugRelated,
        cast(nullif(Suspension_Count_Defiance_Only, '*') as int64)
            as SuspensionCountDefianceOnly,
        cast(nullif(Suspension_Count_Other_Reasons, '*') as int64)
            as SuspensionCountOtherReasons
    from unioned
    where
        AggregateLevel = 'T'
        or (
            AggregateLevel = 'C'
            and CountyCode = 43
        )
        or DistrictCode in (
            10439, --SCCOE
            69369, --ARUSD
            69450, --FMSD
            69666, --SJUSD
            69427 -- ESUHSD
        )

)

select * from final
