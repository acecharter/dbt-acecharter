SELECT DISTINCT * EXCEPT (AceComparisonSchoolCode, AceComparisonSchoolName)
FROM {{ ref('dim_ComparisonEntities')}}
