with caaspp as (
    select * from {{ ref('base_RD__TomsCaasppTested2023Empower')}}
    union all
    select * from {{ ref('base_RD__TomsCaasppTested2023Esperanza')}}
    union all
    select * from {{ ref('base_RD__TomsCaasppTested2023Inspire')}}
    union all
    select * from {{ ref('base_RD__TomsCaasppTested2023HighSchool')}}
),

final as (
    select
        2023 as TestYear,
        '2022-23' as SchoolYear,
        *
    from caaspp
)

select * from final