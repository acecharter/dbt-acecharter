with chrabs_1617 as (
    select * from {{ source('RawData', 'CdeChrAbs1617') }}
),

chrabs_1718 as (
    select * from {{ source('RawData', 'CdeChrAbs1718') }}
),

chrabs_1819 as (
    select * from {{ source('RawData', 'CdeChrAbs1819') }}
),

--No 2019-20 chronic absenteeism file due to the pandemic

chrabs_2021 as (
    select
        Academic_Year as AcademicYear,
        Aggregate_Level as AggregateLevel,
        County_Code as CountyCode,
        District_Code as DistrictCode,
        School_Code as SchoolCode,
        County_Name as CountyName,
        District_Name as DistrictName,
        School_Name as SchoolName,
        Charter_School as CharterSchool,
        Reporting_Category as ReportingCategory,
        ChronicAbsenteeismEligibleCumula,
        ChronicAbsenteeismCount,
        ChronicAbsenteeismRate
    from {{ source('RawData', 'CdeChrAbs2021') }}
),

unioned as (
    select * from chrabs_1617
    union all
    select * from chrabs_1718
    union all
    select * from chrabs_1819
    union all
    select * from chrabs_2021
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
        CharterYN as CharterSchool,
        ReportingCategory,
        cast(ChronicAbsenteeismEligibleCumula as int64)
            as ChronicAbsenteeismEligibleCumula,
        cast(ChronicAbsenteeismCount as int64) as ChronicAbsenteeismCount,
        round(cast(ChronicAbsenteeismRate as float64) / 100, 3)
            as ChronicAbsenteeismRate
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
