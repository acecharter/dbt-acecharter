WITH
    empower AS (
        SELECT * FROM {{ source('RawData', 'CersEmpower2223')}}
    ),

    esperanza AS (
        SELECT * FROM {{ source('RawData', 'CersEsperanza2223')}}
    ),

    inspire AS (
        SELECT * FROM {{ source('RawData', 'CersInspire2223')}}
    ),

    hs AS (
        SELECT * FROM {{ source('RawData', 'CersHighSchool2223')}}
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