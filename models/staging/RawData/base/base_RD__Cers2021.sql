WITH
  empower AS (
    SELECT * FROM {{ source('RawData', 'CersEmpower2021')}}
  ),

  esperanza AS (
    SELECT * FROM {{ source('RawData', 'CersEsperanza2021')}}
  ),

  inspire AS (
    SELECT * FROM {{ source('RawData', 'CersInspire2021')}}
  ),

  hs AS (
    SELECT * FROM {{ source('RawData', 'CersHighSchool2021')}}
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