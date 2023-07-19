{{ config(
    materialized='table'
)}}

with enrollment as (
    select * from {{ ref('base_RD__CdeEnr22')}}
    union all
    select * from {{ ref('base_RD__CdeEnr21')}}
    union all
    select * from {{ ref('base_RD__CdeEnr20')}}
    union all
    select * from {{ ref('base_RD__CdeEnr19')}}
    union all
    select * from {{ ref('base_RD__CdeEnr18')}}
    union all
    select * from {{ ref('base_RD__CdeEnr17')}}
    union all
    select * from {{ ref('base_RD__CdeEnr16')}}
    union all
    select * from {{ ref('base_RD__CdeEnr15')}}
    union all
    select * from {{ ref('base_RD__CdeEnr14')}}
    union all
    select * from {{ ref('base_RD__CdeEnr13')}}
    union all
    select * from {{ ref('base_RD__CdeEnr12')}}
    union all
    select * from {{ ref('base_RD__CdeEnr11')}}
    union all
    select * from {{ ref('base_RD__CdeEnr10')}}
    union all
    select * from {{ ref('base_RD__CdeEnr09')}}
    union all
    select * from {{ ref('base_RD__CdeEnr08')}}
),

final as (
    select
        Year,
        CDS_CODE as CdsCode,
        LEFT(cast(CDS_CODE as string), 2) as CountyCode,
        SUBSTR(cast(CDS_CODE as string), 3, 5) as DistrictCode,
        RIGHT(cast(CDS_CODE as string), 7) as SchoolCode,
        COUNTY as County,
        DISTRICT as District,
        SCHOOL as School,
        cast(ETHNIC as string) as RaceEthnicCode,
        GENDER as Gender,
        * except (
                Year,
                CDS_CODE,
                COUNTY,
                DISTRICT,
                SCHOOL,
                ETHNIC,
                GENDER
        )
    from enrollment
)

select * from final
