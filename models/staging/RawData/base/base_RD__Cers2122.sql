WITH
  empower AS (
    SELECT * FROM {{ source('RawData', 'CersEmpower2122')}}
  ),

  esperanza AS (
    SELECT * FROM {{ source('RawData', 'CersEsperanza2122')}}
  ),

  inspire AS (
    SELECT * FROM {{ source('RawData', 'CersInspire2122')}}
  ),

  hs AS (
    SELECT * FROM {{ source('RawData', 'CersHighSchool2122')}}
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