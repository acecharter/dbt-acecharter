with unioned as (
    select * from {{ ref('base_RD__Calpads217Elas2022') }}
    union all
    select * from {{ ref('base_RD__Calpads217Elas2023') }}
),

final as (
    select
        SchoolYear,
        cast(SchoolCode as string) as SchoolCode,
        cast(SSID as string) as SSID,
        StudentName,
        cast(LocalID as string) as LocalID,
        RFEPDate
    from unioned
)

select * from final