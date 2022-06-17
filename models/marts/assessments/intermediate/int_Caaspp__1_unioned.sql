WITH
  assessment_ids AS (
    SELECT 
      AceAssessmentId,
      AssessmentNameShort AS AceAssessmentName,
      SystemOrVendorAssessmentId
    FROM {{ ref('stg_GSD__Assessments') }}
    WHERE SystemOrVendorName = 'CAASPP'
  ),
  
  caaspp_2015 AS (
    SELECT * FROM {{ ref('stg_RD__Caaspp2015')}}
  ),

  caaspp_2016 AS (
    SELECT * FROM {{ ref('stg_RD__Caaspp2016')}}
  ),

  caaspp_2017 AS (
    SELECT * FROM {{ ref('stg_RD__Caaspp2017')}}
  ),

  caaspp_2018 AS (
    SELECT * FROM {{ ref('stg_RD__Caaspp2018')}}
  ),

  caaspp_2019 AS (
    SELECT * FROM {{ ref('stg_RD__Caaspp2019')}}
  ),

  caaspp_2021 AS (
    SELECT * FROM {{ ref('stg_RD__Caaspp2021')}}
  ),

  unioned AS (
    SELECT * FROM caaspp_2015
    UNION ALL
    SELECT * FROM caaspp_2016
    UNION ALL
    SELECT * FROM caaspp_2017
    UNION ALL
    SELECT * FROM caaspp_2018
    UNION ALL
    SELECT * FROM caaspp_2019
    UNION ALL
    SELECT * FROM caaspp_2021
  ),

  final AS (
    SELECT
      a.AceAssessmentId,
      a.AceAssessmentName,
      CASE
        WHEN u.DistrictCode = '00000' THEN u.CountyCode
        WHEN u.SchoolCode = '0000000' THEN u.DistrictCode
        ELSE u.SchoolCode
      END AS EntityCode,
      u.* EXCEPT (Filler)
    FROM unioned AS u
    LEFT JOIN assessment_ids AS a
    ON u.TestId = a.SystemOrVendorAssessmentId
  )

SELECT * FROM final
