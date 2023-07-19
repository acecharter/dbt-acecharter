select distinct * except (AceComparisonSchoolCode, AceComparisonSchoolName)
from {{ ref('dim_ComparisonEntities') }}
