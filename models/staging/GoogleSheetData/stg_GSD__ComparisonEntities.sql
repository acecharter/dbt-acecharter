SELECT
    CASE EntityType
        WHEN 'State' THEN FORMAT("%02d", EntityCode)
        WHEN 'County' THEN FORMAT("%02d", EntityCode)
        WHEN 'District' THEN FORMAT("%05d", EntityCode)
        WHEN 'School' THEN FORMAT("%07d", EntityCode)
    END AS EntityCode,
    EntityType,
    EntityName,
    EntityNameShort,
    FORMAT("%07d", AceComparisonSchoolCode) AS AceComparisonSchoolCode,
    Notes
FROM {{ source('GoogleSheetData', 'ComparisonEntities')}}