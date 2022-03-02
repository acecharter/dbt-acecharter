WITH 
  caaspp AS (
    SELECT * FROM {{ ref('int_Caaspp__unioned')}} 
  ),

  schools AS (
    SELECT *
    FROM caaspp
    WHERE SchoolCode IN (
      '0116814', --ACE Empower
      '0125617', --ACE High School
      '0129247', --ACE Esperanza
      '0131656', --ACE Inspire
      '6046197', --Lee Mathson Middle (ARUSD)
      '6047229', --Bridges Academy (FMSD)
      '6062103', --Muwekma Ohlone Middle (SJUSD)
      '4330031'  --Independence High (ESUHSD)
    )
  ),

  districts AS (
    SELECT *
    FROM caaspp
    WHERE
      SchoolCode = '0000000'
      AND DistrictCode IN (
        '10439', -- Santa Clara County Office of Education
        '69369', -- Alum Rock Union
        '69450', -- Franklin-McKinley
        '69666', -- San Jose Unified
        '69427'  -- East Side Union
      )
  ),

  county AS (
    SELECT *
    FROM caaspp
    WHERE
      DistrictCode = '00000' 
      AND CountyCode = '43'-- Santa Clara County
  ),

  unioned AS (
    SELECT * FROM schools
    UNION ALL
    SELECT * FROM districts
    UNION ALL
    SELECT * FROM county
  ),

  final AS (
    SELECT *
    FROM unioned
    WHERE
      GradeLevel >= 5 AND
      DemographicId IN (
        '1',   --All Students
        '128', --Reported Disabilities
        '31',  --Economic disadvantaged
        '160', --EL (English learner)
        '78',  --Hispanic or Latino
        '204'  --Economically disadvantaged Hispanic or Latino
      )
  )

SELECT * FROM final