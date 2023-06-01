WITH
    caaspp AS (
        SELECT * FROM {{ ref('base_RD__TomsCaasppTested2022Empower')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsCaasppTested2022Esperanza')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsCaasppTested2022Inspire')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsCaasppTested2022HighSchool')}}
    ),

    final AS (
        SELECT
            2022 AS TestYear,
            '2021-22' AS SchoolYear,
            *
        FROM caaspp
    )

SELECT * FROM final