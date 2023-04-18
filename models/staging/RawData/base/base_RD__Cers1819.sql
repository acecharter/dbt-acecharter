WITH
    empower AS (
        SELECT * FROM {{ source('RawData', 'CersEmpower1819')}}
    ),

    esperanza AS (
        SELECT * FROM {{ source('RawData', 'CersEsperanza1819')}}
    ),

    inspire AS (
        SELECT * FROM {{ source('RawData', 'CersInspire1819')}}
    ),

    hs AS (
        SELECT * FROM {{ source('RawData', 'CersHighSchool1819')}}
    ),

    final AS (
        SELECT * FROM empower
        UNION ALL
        SELECT * FROM esperanza
        UNION ALL
        SELECT * FROM inspire
        UNION ALL
        SELECT * FROM hs
    )

SELECT DISTINCT * FROM final