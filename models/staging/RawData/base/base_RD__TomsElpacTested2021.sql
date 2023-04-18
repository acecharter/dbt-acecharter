WITH
    elpac AS (
        SELECT * FROM {{ ref('base_RD__TomsElpacTested2021Empower')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsElpacTested2021Esperanza')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsElpacTested2021Inspire')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsElpacTested2021HighSchool')}}
    ),

    final AS (
        SELECT
            2021 AS TestYear,
            '2020-21' AS SchoolYear,
            *,
            CAST(NULL AS STRING) AS AttemptednessMinus3,
            CAST(NULL AS STRING) AS GradeAssessedMinus3,
            CAST(NULL AS STRING) AS OverallScaleScoreMinus3,
            CAST(NULL AS STRING) AS OverallPLMinus3,
            CAST(NULL AS STRING) AS OralLanguagePLMinus3,
            CAST(NULL AS STRING) AS WrittenLanguagePLMinus3,
            CAST(NULL AS STRING) AS ListeningPLMinus3,
            CAST(NULL AS STRING) AS SpeakingPLMinus3,
            CAST(NULL AS STRING) AS ReadingPLMinus3,
            CAST(NULL AS STRING) AS WritingPLMinus3
        FROM elpac
    )

SELECT * FROM final