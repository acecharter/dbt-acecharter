select
    SchoolYear,
    case
        when EntityType in ('State', 'County') then format('%02d', EntityCode)
        when EntityType = 'District' then format('%05d', EntityCode)
        when EntityType = 'School' then format('%07d', EntityCode)
    end as EntityCode,
    EntityType,
    EntityName,
    Subgroup,
    Charter_School_Enrollment as CharterSchoolEnrollment,
    Non_Charter_School_Enrollment as NonCharterSchoolEnrollment,
    Total_Enrollment as TotalEnrollment
from {{ source('GoogleSheetData', 'CdeEnrBySubgroupEntities') }}
