SELECT
  CASE
    WHEN EntityType = 'State' THEN FORMAT("%01d", EntityCode)
    WHEN EntityType = 'County' THEN FORMAT("%02d", EntityCode)
    WHEN EntityType = 'District' THEN FORMAT("%05d", EntityCode)
    WHEN EntityType = 'School' THEN FORMAT("%07d", EntityCode)
  END AS EntityCode,
  EntityType,
  EntityName,
  EntityNameShort,
  FORMAT("%07d", AceComparisonSchoolCode) AS AceComparisonSchoolCode,
  Notes
FROM {{ source('GoogleSheetData', 'ComparisonEntities')}}