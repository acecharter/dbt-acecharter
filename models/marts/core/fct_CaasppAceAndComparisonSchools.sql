WITH ace AS (
  SELECT *
  FROM {{ ref('int_CaasppSantaClaraCounty_CaasppEntities_joined')}}
  WHERE SchoolCode IN (
    '0116814', --ACE Empower
    '0125617', --ACE High School
    '0129247', --ACE Esperanza
    '0131656' --ACE Inspire
  )
),

comp_schools AS (
  SELECT *
  FROM {{ ref('int_CaasppSantaClaraCounty_CaasppEntities_joined')}}
  WHERE SchoolCode IN (
    '6046197', --Lee Mathson Middle (ARUSD)
    '6047229', --Bridges Academy (FMSD)
    '6062103', --Muwekma Ohlone Middle (SJUSD)
    '4330031'  --Independence High (ESUHSD)
  )
),

comp_districts AS (
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

comp_county AS (
  SELECT *
  FROM {{ ref('int_CaasppSantaClaraCounty_CaasppEntities_joined')}}
  WHERE DistrictCode = '00000' -- Santa Clara County
),

final AS (
  SELECT * FROM ace
  UNION ALL
  SELECT * FROM comp_schools
  UNION ALL
  SELECT * FROM comp_districts
  UNION ALL
  SELECT * FROM comp_county
)

SELECT * FROM final
