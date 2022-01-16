WITH
schools AS (
  SELECT *
  FROM {{ ref('int_CaasppSantaClaraCounty_CaasppEntities_joined')}}
  WHERE SchoolCode IN (
    '6046197', --Lee Mathson Middle (ARUSD)
    '6047229', --Bridges Academy (FMSD)
    '6062103', --Muwekma Ohlone Middle (SJUSD)
    '4330031'  --Independence High (ESUHSD)
  )
),

districts AS (
  SELECT *
  FROM {{ ref('int_CaasppSantaClaraCounty_CaasppEntities_joined')}}
  WHERE
    SchoolCode = '0000000' AND
    DistrictCode IN (
      '10439', -- Santa Clara County Office of Education
      '69369', -- Alum Rock Union
      '69450', -- Franklin-McKinley
      '69666', -- San Jose Unified
      '69427' -- East Side Union
    )
),

county AS (
  SELECT *
  FROM {{ ref('int_CaasppSantaClaraCounty_CaasppEntities_joined')}}
  WHERE DistrictCode = '00000' -- Santa Clara County
),


SELECT * FROM schools
UNION ALL
SELECT * FROM districts
UNION ALL
SELECT * FROM county

