WITH
    caaspp AS (
        SELECT * FROM {{ ref('base_RD__TomsCaasppTested2023Empower')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsCaasppTested2023Esperanza')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsCaasppTested2023Inspire')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsCaasppTested2023HighSchool')}}
    ),

    final AS (
        SELECT
            2022 AS TestYear,
            '2021-22' AS SchoolYear,
            *
        FROM caaspp
    )

SELECT * FROM final