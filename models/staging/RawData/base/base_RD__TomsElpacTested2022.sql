with elpac as (
    select * from {{ ref('base_RD__TomsElpacTested2022Empower')}}
    union all select * from {{ ref('base_RD__TomsElpacTested2022Esperanza')}}
    union all select * from {{ ref('base_RD__TomsElpacTested2022Inspire')}}
    union all select * from {{ ref('base_RD__TomsElpacTested2022HighSchool')}}
),

final as (
    select
        2022 as TestYear,
        '2021-22' as SchoolYear,
        *
    from elpac
)

select * from final
