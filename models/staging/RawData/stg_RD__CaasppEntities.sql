with unioned as (
    select * from {{ source('RawData', 'CaasppEntities2015') }}
    union all
    select * from {{ source('RawData', 'CaasppEntities2016') }}
    union all
    select * from {{ source('RawData', 'CaasppEntities2017') }}
    union all
    select * from {{ source('RawData', 'CaasppEntities2018') }}
    union all
    select * from {{ source('RawData', 'CaasppEntities2019') }}
    union all
    select
        * except (Zip_Code),
        cast(Zip_Code as string) as Zip_Code
    from {{ source('RawData', 'CaasppEntities2021') }}
),

formatted as (
    select
        format('%02d', County_Code) as CountyCode,
        format('%05d', District_Code) as DistrictCode,
        format('%07d', School_Code) as SchoolCode,
        Test_Year as TestYear,
        cast(Type_Id as string) as TypeId,
        County_Name as CountyName,
        District_Name as DistrictName,
        School_Name as SchoolName,
        case
            when replace(Zip_Code, ' ', '') = '' then null
            else cast(Zip_Code as string)
        end as ZipCode
    from unioned
),

final as (
    select
        case
            when DistrictCode = '00000' then CountyCode
            when SchoolCode = '0000000' then DistrictCode
            else SchoolCode
        end as EntityCode,
        case
            when CountyCode = '00' then 'State'
            when CountyCode = '43' and DistrictCode = '00000' then 'County'
            when SchoolCode = '0000000' then 'District'
            else 'School'
        end as EntityType,
        *
    from formatted

)

select * from final
