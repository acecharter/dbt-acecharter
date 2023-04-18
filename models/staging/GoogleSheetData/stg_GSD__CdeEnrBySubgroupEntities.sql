SELECT
    SchoolYear,
    CASE
        WHEN EntityType IN ('State', 'County') THEN FORMAT("%02d", EntityCode)
        WHEN EntityType = 'District' THEN FORMAT("%05d", EntityCode)
        WHEN EntityType = 'School' THEN FORMAT("%07d", EntityCode)
    END AS EntityCode,
    EntityType,
    EntityName,
    Subgroup,
    Charter_School_Enrollment AS CharterSchoolEnrollment,
    Non_Charter_School_Enrollment AS NonCharterSchoolEnrollment,
    Total_Enrollment AS TotalEnrollment
FROM {{ source('GoogleSheetData', 'CdeEnrBySubgroupEntities')}}