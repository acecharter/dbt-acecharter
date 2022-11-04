WITH
  empower AS (
    SELECT * FROM {{ source('RawData', 'CersEmpower1920')}}
  ),

  esperanza AS (
    SELECT * FROM {{ source('RawData', 'CersEsperanza1920')}}
  ),

  inspire AS (
    SELECT * FROM {{ source('RawData', 'CersInspire1920')}}
  ),

  hs AS (
    SELECT * FROM {{ source('RawData', 'CersHighSchool1920')}}
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