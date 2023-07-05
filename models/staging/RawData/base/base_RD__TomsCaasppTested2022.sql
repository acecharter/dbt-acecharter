with caaspp as (
    select * from {{ ref('base_RD__TomsCaasppTested2022Empower')}}
    union all select * from {{ ref('base_RD__TomsCaasppTested2022Esperanza')}}
    union all select * from {{ ref('base_RD__TomsCaasppTested2022Inspire')}}
    union all select * from {{ ref('base_RD__TomsCaasppTested2022HighSchool')}}
),

final as (
    select
        2022 as TestYear,
        '2021-22' as SchoolYear,
        *
    from caaspp
)

select * from final