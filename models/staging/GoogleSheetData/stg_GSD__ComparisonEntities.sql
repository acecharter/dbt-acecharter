select
    case EntityType
        when 'State' then format('%02d', EntityCode)
        when 'County' then format('%02d', EntityCode)
        when 'District' then format('%05d', EntityCode)
        when 'School' then format('%07d', EntityCode)
    end as EntityCode,
    EntityType,
    EntityName,
    EntityNameShort,
    format('%07d', AceComparisonSchoolCode) as AceComparisonSchoolCode,
    Notes
from {{ source('GoogleSheetData', 'ComparisonEntities') }}
