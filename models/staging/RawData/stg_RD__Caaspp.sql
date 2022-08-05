WITH
  assessment_ids AS (
    SELECT 
      AceAssessmentId,
      AssessmentNameShort AS AceAssessmentName,
      SystemOrVendorAssessmentId
    FROM {{ ref('stg_GSD__Assessments') }}
    WHERE SystemOrVendorName = 'CAASPP'
  ),
  
  entities AS (
    SELECT * FROM {{ ref('dim_Entities')}}
  ),

  unioned AS (
    SELECT * FROM {{ ref('base_RD__Caaspp2015')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Caaspp2016')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Caaspp2017')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Caaspp2018')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Caaspp2019')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Caaspp2021')}}
  ),

  caaspp AS (
    SELECT
      *,
      CASE
        WHEN DistrictCode = '00000' THEN CountyCode
        WHEN SchoolCode = '0000000' THEN DistrictCode
        ELSE SchoolCode
      END AS EntityCode,
    FROM unioned
  ),

  final AS (
    SELECT
      a.AceAssessmentId,
      a.AceAssessmentName,
      e.*,
      c.* EXCEPT(Filler, EntityCode)
    FROM caaspp AS c
    LEFT JOIN assessment_ids AS a
    ON c.TestId = a.SystemOrVendorAssessmentId
    LEFT JOIN entities as e
    ON c.EntityCode = e.EntityCode
    WHERE c.EntityCode IN (SELECT EntityCode FROM entities)
  )

SELECT * FROM final
  