with
    elpac as (
        select * from {{ ref('base_RD__TomsElpacTested2023Empower')}}
        union all select * from {{ ref('base_RD__TomsElpacTested2023Esperanza')}}
        union all select * from {{ ref('base_RD__TomsElpacTested2023Inspire')}}
        union all select * from {{ ref('base_RD__TomsElpacTested2023HighSchool')}}
    ),

    final as (
        select
            2023 as TestYear,
            '2022-23' as SchoolYear,
            *
        from elpac
    )

select * from final
