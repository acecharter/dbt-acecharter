WITH
  cgr AS (
    SELECT * FROM {{ ref('int_CdeCgr__unioned')}}
  ),

  entity_names AS (
    SELECT
      EntityType,
      CountyCode,
      DistrictCode,
      SchoolCode,
      CountyName,
      DistrictName,
      SchoolName,
      MAX(AcademicYear) AS MaxAcademicYear
    FROM cgr
    GROUP BY 1, 2, 3, 4, 5, 6, 7
  ),

  final AS (
    SELECT
      EntityType,
      CASE
        WHEN EntityType = 'State' THEN '00'
        WHEN EntityType = 'County' THEN CountyCode
        WHEN EntityType = 'District' THEN DistrictCode
        WHEN EntityType = 'School' THEN SchoolCode
      END AS EntityCode,
      CASE
        WHEN EntityType IN ('State', 'County') THEN CountyName
        WHEN EntityType = 'District' THEN DistrictName
        WHEN EntityType = 'School' THEN SchoolName
      END AS EntityName
    FROM entity_names
  )

SELECT *
FROM final
ORDER BY 1, 2
