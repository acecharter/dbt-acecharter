WITH
  elpac AS (
    SELECT * FROM {{ ref('base_RD__TomsElpacTested2022Empower')}}
    UNION ALL SELECT * FROM {{ ref('base_RD__TomsElpacTested2022Esperanza')}}
    UNION ALL SELECT * FROM {{ ref('base_RD__TomsElpacTested2022Inspire')}}
    UNION ALL SELECT * FROM {{ ref('base_RD__TomsElpacTested2022HighSchool')}}
  ),

  final AS (
    SELECT
      2022 AS TestYear,
      '2021-22' AS SchoolYear,
      *
    FROM elpac
  )

SELECT * FROM final