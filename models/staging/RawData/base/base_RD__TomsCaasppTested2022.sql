WITH
  empower AS (
    SELECT * FROM {{ ref('base_RD__TomsCaasppTested2022Empower')}}
  ),
  esperanza AS (
    SELECT * FROM {{ ref('base_RD__TomsCaasppTested2022Esperanza')}}
  ),
  inspire AS (
    SELECT * FROM {{ ref('base_RD__TomsCaasppTested2022Inspire')}}
  ),
  hs AS (
    SELECT * FROM {{ ref('base_RD__TomsCaasppTested2022HighSchool')}}
  ),

  unioned AS (
      SELECT * FROM empower
      UNION ALL
      SELECT * FROM esperanza
      UNION ALL
      SELECT * FROM inspire
      UNION ALL
      SELECT * FROM hs
  )

SELECT * FROM unioned