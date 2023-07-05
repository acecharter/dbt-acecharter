select
    SchoolYear,
    case
        when EntityType in ('State', 'County') then format('%02d', EntityCode)
        when EntityType = 'District' then format('%05d', EntityCode)
    end as EntityCode,
    EntityType,
    EntityName,
    Subgroup,
    SchoolType,
    Ethnicity,
    Grade_K as GR_K,
    Grade_1 as GR_1,
    Grade_2 as GR_2,
    Grade_3 as GR_3,
    Grade_4 as GR_4,
    Grade_5 as GR_5,
    Grade_6 as GR_6,
    Grade_7 as GR_7,
    Grade_8 as GR_8,
    Ungr_Elem as UNGR_ELEM,
    Grade_9 as GR_9,
    Grade_10 as GR_10,
    Grade_11 as GR_11,
    Grade_12 as GR_12,
    Ungr_Sec as UNGR_SEC,
    Total as ENR_TOTAL
from {{ source('GoogleSheetData', 'CdeEnrByRaceAndGradeEntities') }}
