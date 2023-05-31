WITH
    caaspp AS (
        SELECT * FROM {{ ref('base_RD__TomsCaasppTested2023Empower')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsCaasppTested2023Esperanza')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsCaasppTested2023Inspire')}}
        UNION ALL SELECT * FROM {{ ref('base_RD__TomsCaasppTested2023HighSchool')}}
    ),

    final AS (
        SELECT
            2023 AS TestYear,
            '2022-23' AS SchoolYear,
            *
        FROM caaspp
    )

SELECT * FROM final