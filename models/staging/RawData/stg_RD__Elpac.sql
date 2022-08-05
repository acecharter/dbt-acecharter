WITH
  entities AS (
    SELECT * FROM {{ ref('dim_Entities')}}
  ),

  assessment_ids AS (
    SELECT 
      AceAssessmentId,
      AssessmentNameShort AS AceAssessmentName,
      SystemOrVendorAssessmentId
    FROM {{ ref('stg_GSD__Assessments') }}
    WHERE SystemOrVendorName = 'ELPAC'
  ),

  elpac_unioned AS (
    SELECT * FROM {{ ref('base_RD__Elpac2018')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Elpac2019')}}
    UNION ALL
    SELECT * FROM {{ ref('base_RD__Elpac2021')}}
  ),

  elpac_entity_codes_added AS (
    SELECT
      a.AceAssessmentId,
      a.AceAssessmentName,
      CASE
        WHEN u.DistrictCode = '00000' THEN u.CountyCode
        WHEN u.SchoolCode = '0000000' THEN u.DistrictCode
        ELSE u.SchoolCode
      END AS EntityCode,
      u.*
    FROM elpac_unioned AS u
    LEFT JOIN assessment_ids AS a
    ON u.AssessmentType = a.SystemOrVendorAssessmentId
  ),

  elpac_filtered AS (
    SELECT *
    FROM elpac_entity_codes_added
    WHERE EntityCode IN (SELECT EntityCode FROM entities)
      
  ),

  final AS (
    SELECT
      elpac.*,
      entities.* EXCEPT (EntityCode), 
      CONCAT(
        CAST(elpac.TestYear - 1 AS STRING), '-', CAST(elpac.TestYear - 2000 AS STRING)
      ) AS SchoolYear,
    FROM elpac
    LEFT JOIN entities
    ON elpac.EntityCode = entities.EntityCode
  )

SELECT * FROM final
