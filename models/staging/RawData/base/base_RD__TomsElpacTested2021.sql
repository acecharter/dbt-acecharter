with
    elpac as (
        select * from {{ ref('base_RD__TomsElpacTested2021Empower')}}
        union all select * from {{ ref('base_RD__TomsElpacTested2021Esperanza')}}
        union all select * from {{ ref('base_RD__TomsElpacTested2021Inspire')}}
        union all select * from {{ ref('base_RD__TomsElpacTested2021HighSchool')}}
    ),

    final as (
        select
            2021 as TestYear,
            '2020-21' as SchoolYear,
            *,
            cast(null as string) as AttemptednessMinus3,
            cast(null as string) as GradeAssessedMinus3,
            cast(null as string) as OverallScaleScoreMinus3,
            cast(null as string) as OverallPLMinus3,
            cast(null as string) as OralLanguagePLMinus3,
            cast(null as string) as WrittenLanguagePLMinus3,
            cast(null as string) as ListeningPLMinus3,
            cast(null as string) as SpeakingPLMinus3,
            cast(null as string) as ReadingPLMinus3,
            cast(null as string) as WritingPLMinus3
        from elpac
    )

select * from final
