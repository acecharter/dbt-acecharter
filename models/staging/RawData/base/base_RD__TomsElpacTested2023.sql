WITH
    elpac AS (
        SELECT * FROM {{ ref('base_RD__TomsElpacTested2023Empower')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsElpacTested2023Esperanza')}}
        --UNION ALL SELECT * FROM {{ ref('base_RD__TomsElpacTested2023Inspire')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsElpacTested2023HighSchool')}}
    ),

    final AS (
        SELECT
            2023 AS TestYear,
            '2022-23' AS SchoolYear,
            *
        FROM elpac
    )

SELECT * FROM final