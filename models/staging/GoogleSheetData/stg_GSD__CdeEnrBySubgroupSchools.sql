select
    format("%07d", SchoolCode) as SchoolCode,
    * except (SchoolCode)
from {{ source('GoogleSheetData', 'CdeEnrBySubgroupSchools') }}
