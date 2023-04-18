SELECT
    SchoolYear,
    CASE
        WHEN EntityType IN ('State', 'County') THEN FORMAT("%02d", EntityCode)
        WHEN EntityType = 'District' THEN FORMAT("%05d", EntityCode)
    END AS EntityCode,
    EntityType,
    EntityName,
    Subgroup,
    SchoolType,
    Ethnicity,
    Grade_K AS GR_K,
    Grade_1 AS GR_1,
    Grade_2 AS GR_2,
    Grade_3 AS GR_3,
    Grade_4 AS GR_4,
    Grade_5 AS GR_5,
    Grade_6 AS GR_6,
    Grade_7 AS GR_7,
    Grade_8 AS GR_8,
    Ungr_Elem AS UNGR_ELEM,
    Grade_9 AS GR_9,
    Grade_10 AS GR_10,
    Grade_11 AS GR_11,
    Grade_12 AS GR_12,
    Ungr_Sec AS UNGR_SEC,
    Total AS ENR_TOTAL
FROM {{ source('GoogleSheetData', 'CdeEnrByRaceAndGradeEntities')}}